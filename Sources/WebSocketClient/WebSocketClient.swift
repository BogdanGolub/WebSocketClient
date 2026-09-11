import Foundation

/// A WebSocket client that survives real networks.
///
/// - Reconnects with exponential backoff and jitter (`ReconnectPolicy`).
/// - Detects dead connections with periodic pings and a pong timeout.
/// - Resumes from the last seen event id, so the server can replay what was missed.
/// - Queues outgoing messages (bounded) while offline and flushes them on connect.
///
/// The client is an actor: every call is safe from any task. Observe `events`
/// for state changes and incoming messages.
public actor WebSocketClient {

    // MARK: Configuration

    public struct Configuration: Sendable {
        /// Extra HTTP headers for the handshake.
        public var headers: [String: String]
        public var reconnect: ReconnectPolicy
        /// Interval between pings. `nil` disables heartbeats.
        public var heartbeatInterval: Duration?
        /// How long to wait for a pong before declaring the connection dead.
        public var heartbeatTimeout: Duration
        /// How the last seen event id is sent back on reconnect. `nil` disables resume.
        public var resume: ResumeStrategy?
        /// Upper bound of the offline queue; `send` throws `ClientError.pendingQueueFull` beyond it.
        public var maxPendingMessages: Int

        public init(
            headers: [String: String] = [:],
            reconnect: ReconnectPolicy = .default,
            heartbeatInterval: Duration? = .seconds(20),
            heartbeatTimeout: Duration = .seconds(10),
            resume: ResumeStrategy? = nil,
            maxPendingMessages: Int = 100
        ) {
            self.headers = headers
            self.reconnect = reconnect
            self.heartbeatInterval = heartbeatInterval
            self.heartbeatTimeout = heartbeatTimeout
            self.resume = resume
            self.maxPendingMessages = maxPendingMessages
        }
    }

    /// Where the last event id goes when reconnecting, and how to read it from a message.
    public struct ResumeStrategy: Sendable {
        public enum Placement: Sendable {
            case queryItem(String)
            case header(String)
        }

        public var placement: Placement
        /// Extracts an event id from an incoming message; return `nil` for messages without one.
        public var eventID: @Sendable (WebSocketMessage) -> String?

        public init(placement: Placement, eventID: @escaping @Sendable (WebSocketMessage) -> String?) {
            self.placement = placement
            self.eventID = eventID
        }
    }

    // MARK: State and events

    public enum State: Sendable, Equatable {
        case idle
        case connecting
        case connected
        case waitingToReconnect(attempt: Int)
        /// Finished: either `disconnect()` was called or retries were exhausted.
        case closed
    }

    public enum Event: Sendable {
        case stateChanged(State)
        case message(WebSocketMessage)
        /// A connection attempt or an open connection failed; a reconnect follows unless retries are exhausted.
        case connectionFailed(any Error)
        case retriesExhausted
    }

    public enum ClientError: Error, Sendable, Equatable {
        case pendingQueueFull
        case heartbeatTimeout
        case closed
    }

    public nonisolated let url: URL
    public nonisolated let configuration: Configuration
    /// State changes, incoming messages and failures, in order.
    public nonisolated let events: AsyncStream<Event>

    public private(set) var state: State = .idle {
        didSet {
            if state != oldValue {
                eventContinuation.yield(.stateChanged(state))
            }
        }
    }

    /// The most recent event id seen, as extracted by `Configuration.resume`.
    public private(set) var lastEventID: String?

    private let transport: any WebSocketTransport
    private let clock = ContinuousClock()
    private let eventContinuation: AsyncStream<Event>.Continuation
    private var runTask: Task<Void, Never>?
    private var connection: (any WebSocketConnection)?
    private var pending: [WebSocketMessage] = []

    public init(url: URL, transport: any WebSocketTransport, configuration: Configuration = Configuration()) {
        self.url = url
        self.transport = transport
        self.configuration = configuration
        let (stream, continuation) = AsyncStream.makeStream(of: Event.self)
        self.events = stream
        self.eventContinuation = continuation
    }

    // MARK: Public API

    /// Starts connecting and returns immediately. Reconnects automatically until
    /// `disconnect()` is called or the reconnect policy gives up.
    public func connect() {
        guard runTask == nil, state != .closed else { return }
        runTask = Task { await self.run() }
    }

    /// Stops reconnecting and closes the current connection.
    public func disconnect() async {
        runTask?.cancel()
        runTask = nil
        if let connection {
            await connection.close()
            self.connection = nil
        }
        state = .closed
    }

    /// Sends a message, or queues it while offline. Queued messages are flushed in order
    /// as soon as a connection is established.
    public func send(_ message: WebSocketMessage) async throws {
        if state == .closed {
            throw ClientError.closed
        }
        if state == .connected, let connection {
            do {
                try await connection.send(message)
                return
            } catch {
                // The receive loop will notice the broken connection and reconnect;
                // keep the message so it goes out on the next connection.
            }
        }
        try enqueue(message)
    }

    // MARK: Connection loop

    private func run() async {
        var attempt = 0
        while !Task.isCancelled {
            state = .connecting
            do {
                let connection = try await transport.connect(to: resumeURL(), headers: resumeHeaders())
                self.connection = connection
                attempt = 0
                state = .connected
                try await flushPending(to: connection)
                try await pump(connection)
            } catch is CancellationError {
                break
            } catch {
                eventContinuation.yield(.connectionFailed(error))
            }

            if let connection {
                await connection.close()
                self.connection = nil
            }
            if Task.isCancelled {
                break
            }
            guard let delay = configuration.reconnect.delay(forAttempt: attempt) else {
                eventContinuation.yield(.retriesExhausted)
                break
            }
            attempt += 1
            state = .waitingToReconnect(attempt: attempt)
            do {
                try await clock.sleep(for: delay)
            } catch {
                break
            }
        }
        state = .closed
        runTask = nil
    }

    /// Runs the receive loop and the heartbeat side by side; returns only by throwing,
    /// which is how a dead or dropped connection surfaces to `run()`.
    private func pump(_ connection: any WebSocketConnection) async throws {
        let heartbeatInterval = configuration.heartbeatInterval
        let heartbeatTimeout = configuration.heartbeatTimeout
        let clock = self.clock

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { [weak self] in
                while true {
                    let message = try await connection.receive()
                    await self?.handleIncoming(message)
                }
            }
            if let heartbeatInterval {
                group.addTask {
                    while true {
                        try await clock.sleep(for: heartbeatInterval)
                        try await Self.ping(connection, timeout: heartbeatTimeout, clock: clock)
                    }
                }
            }
            // Neither child returns normally; the first failure propagates and cancels the other.
            _ = try await group.next()
            group.cancelAll()
        }
    }

    private static func ping(_ connection: any WebSocketConnection, timeout: Duration, clock: ContinuousClock) async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await connection.ping()
            }
            group.addTask {
                try await clock.sleep(for: timeout)
                throw ClientError.heartbeatTimeout
            }
            _ = try await group.next()
            group.cancelAll()
        }
    }

    private func handleIncoming(_ message: WebSocketMessage) {
        if let resume = configuration.resume, let id = resume.eventID(message) {
            lastEventID = id
        }
        eventContinuation.yield(.message(message))
    }

    private func enqueue(_ message: WebSocketMessage) throws {
        guard pending.count < configuration.maxPendingMessages else {
            throw ClientError.pendingQueueFull
        }
        pending.append(message)
    }

    private func flushPending(to connection: any WebSocketConnection) async throws {
        while !pending.isEmpty {
            let message = pending.removeFirst()
            do {
                try await connection.send(message)
            } catch {
                pending.insert(message, at: 0)
                throw error
            }
        }
    }

    // MARK: Resume

    private func resumeURL() -> URL {
        guard let resume = configuration.resume,
              case .queryItem(let name) = resume.placement,
              let lastEventID,
              var components = URLComponents(url: url, resolvingAgainstBaseURL: false)
        else {
            return url
        }
        var items = components.queryItems ?? []
        items.removeAll { $0.name == name }
        items.append(URLQueryItem(name: name, value: lastEventID))
        components.queryItems = items
        return components.url ?? url
    }

    private func resumeHeaders() -> [String: String] {
        var headers = configuration.headers
        if let resume = configuration.resume,
           case .header(let name) = resume.placement,
           let lastEventID {
            headers[name] = lastEventID
        }
        return headers
    }
}

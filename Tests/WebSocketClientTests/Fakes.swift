import Foundation
@testable import WebSocketClient

struct ConnectionDropped: Error, Equatable {}
struct HandshakeFailed: Error, Equatable {}

/// Scripts the outcome of consecutive `connect` calls and records what the client asked for.
actor FakeTransport: WebSocketTransport {
    enum Attempt {
        case succeed(FakeConnection)
        case fail(any Error)
    }

    struct ConnectCall {
        var url: URL
        var headers: [String: String]
    }

    private var script: [Attempt]
    private(set) var calls: [ConnectCall] = []

    init(script: [Attempt]) {
        self.script = script
    }

    func connect(to url: URL, headers: [String: String]) async throws -> any WebSocketConnection {
        calls.append(ConnectCall(url: url, headers: headers))
        guard !script.isEmpty else {
            throw HandshakeFailed()
        }
        switch script.removeFirst() {
        case .succeed(let connection):
            return connection
        case .fail(let error):
            throw error
        }
    }
}

/// An in-memory connection the test drives from the "server" side:
/// push messages, drop the connection, or make pings hang.
actor FakeConnection: WebSocketConnection {
    enum PingBehavior {
        case succeed
        case fail
        case hang
    }

    private var buffer: [WebSocketMessage] = []
    private var waiter: CheckedContinuation<WebSocketMessage, any Error>?
    private var dropError: (any Error)?
    private var pingBehavior: PingBehavior = .succeed

    private(set) var sent: [WebSocketMessage] = []
    private(set) var pings = 0
    private(set) var isClosed = false

    // MARK: Server side

    func push(_ message: WebSocketMessage) {
        if let waiter {
            self.waiter = nil
            waiter.resume(returning: message)
        } else {
            buffer.append(message)
        }
    }

    func drop(_ error: any Error = ConnectionDropped()) {
        dropError = error
        if let waiter {
            self.waiter = nil
            waiter.resume(throwing: error)
        }
    }

    func setPingBehavior(_ behavior: PingBehavior) {
        pingBehavior = behavior
    }

    // MARK: WebSocketConnection

    func send(_ message: WebSocketMessage) async throws {
        if let dropError {
            throw dropError
        }
        sent.append(message)
    }

    func receive() async throws -> WebSocketMessage {
        if !buffer.isEmpty {
            return buffer.removeFirst()
        }
        if let dropError {
            throw dropError
        }
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<WebSocketMessage, any Error>) in
                waiter = continuation
                if Task.isCancelled {
                    cancelWaiter()
                }
            }
        } onCancel: {
            Task { await self.cancelWaiter() }
        }
    }

    func ping() async throws {
        pings += 1
        switch pingBehavior {
        case .succeed:
            return
        case .fail:
            throw ConnectionDropped()
        case .hang:
            try await Task.sleep(for: .seconds(60))
        }
    }

    func close() {
        isClosed = true
        drop()
    }

    private func cancelWaiter() {
        if let waiter {
            self.waiter = nil
            waiter.resume(throwing: CancellationError())
        }
    }
}

/// Records everything the client emits.
actor EventLog {
    private(set) var events: [WebSocketClient.Event] = []

    func append(_ event: WebSocketClient.Event) {
        events.append(event)
    }

    var messages: [WebSocketMessage] {
        events.compactMap { event -> WebSocketMessage? in
            if case .message(let message) = event {
                return message
            }
            return nil
        }
    }

    var retriesExhausted: Bool {
        events.contains { event in
            if case .retriesExhausted = event {
                return true
            }
            return false
        }
    }

    var failures: Int {
        events.filter { event in
            if case .connectionFailed = event {
                return true
            }
            return false
        }.count
    }
}

/// Polls `condition` until it holds or `timeout` elapses.
func eventually(
    timeout: Duration = .seconds(3),
    _ condition: @Sendable () async -> Bool
) async -> Bool {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
        if await condition() {
            return true
        }
        try? await clock.sleep(for: .milliseconds(5))
    }
    return await condition()
}

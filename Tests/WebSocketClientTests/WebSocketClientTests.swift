import Foundation
import Testing
@testable import WebSocketClient

@Suite("WebSocketClient")
struct WebSocketClientTests {
    private let url = URL(string: "wss://example.com/socket?room=42")!

    /// Fast reconnects, no heartbeat unless a test opts in.
    private func configuration(
        heartbeatInterval: Duration? = nil,
        heartbeatTimeout: Duration = .seconds(10),
        reconnect: ReconnectPolicy = .immediate,
        resume: WebSocketClient.ResumeStrategy? = nil,
        maxPendingMessages: Int = 100
    ) -> WebSocketClient.Configuration {
        WebSocketClient.Configuration(
            headers: ["Authorization": "Bearer token"],
            reconnect: reconnect,
            heartbeatInterval: heartbeatInterval,
            heartbeatTimeout: heartbeatTimeout,
            resume: resume,
            maxPendingMessages: maxPendingMessages
        )
    }

    private func record(_ client: WebSocketClient) -> (EventLog, Task<Void, Never>) {
        let log = EventLog()
        let task = Task {
            for await event in client.events {
                await log.append(event)
            }
        }
        return (log, task)
    }

    @Test("connects, passes headers and delivers incoming messages")
    func connectsAndDelivers() async throws {
        let connection = FakeConnection()
        let transport = FakeTransport(script: [.succeed(connection)])
        let client = WebSocketClient(url: url, transport: transport, configuration: configuration())
        let (log, recorder) = record(client)
        defer { recorder.cancel() }

        await client.connect()
        #expect(await eventually { await client.state == .connected })

        let call = try #require(await transport.calls.first)
        #expect(call.url == url)
        #expect(call.headers["Authorization"] == "Bearer token")

        await connection.push(.text("hello"))
        await connection.push(.data(Data([1, 2, 3])))
        #expect(await eventually { await log.messages.count == 2 })
        #expect(await log.messages == [.text("hello"), .data(Data([1, 2, 3]))])

        await client.disconnect()
        #expect(await client.state == .closed)
        #expect(await connection.isClosed)
    }

    @Test("reconnects after the connection drops and reports the failure")
    func reconnectsAfterDrop() async throws {
        let first = FakeConnection()
        let second = FakeConnection()
        let transport = FakeTransport(script: [.succeed(first), .succeed(second)])
        let client = WebSocketClient(url: url, transport: transport, configuration: configuration())
        let (log, recorder) = record(client)
        defer { recorder.cancel() }

        await client.connect()
        #expect(await eventually { await client.state == .connected })

        await first.drop()
        #expect(await eventually { await transport.calls.count == 2 })
        #expect(await eventually { await client.state == .connected })
        #expect(await first.isClosed)
        #expect(await eventually { await log.failures == 1 })

        await second.push(.text("after reconnect"))
        #expect(await eventually { await log.messages == [.text("after reconnect")] })

        await client.disconnect()
    }

    @Test("sends the last event id as a query item when reconnecting")
    func resumesWithQueryItem() async throws {
        let first = FakeConnection()
        let second = FakeConnection()
        let transport = FakeTransport(script: [.succeed(first), .succeed(second)])
        let resume = WebSocketClient.ResumeStrategy(placement: .queryItem("last_event_id")) { message in
            if case .text(let text) = message {
                return text
            }
            return nil
        }
        let client = WebSocketClient(url: url, transport: transport, configuration: configuration(resume: resume))
        let (_, recorder) = record(client)
        defer { recorder.cancel() }

        await client.connect()
        #expect(await eventually { await client.state == .connected })
        let firstCall = try #require(await transport.calls.first)
        #expect(firstCall.url == url)

        await first.push(.text("17"))
        await first.push(.data(Data()))
        #expect(await eventually { await client.lastEventID == "17" })

        await first.drop()
        #expect(await eventually { await transport.calls.count == 2 })

        let secondCall = try #require(await transport.calls.last)
        let components = try #require(URLComponents(url: secondCall.url, resolvingAgainstBaseURL: false))
        let items = components.queryItems ?? []
        #expect(items.contains(URLQueryItem(name: "room", value: "42")))
        #expect(items.contains(URLQueryItem(name: "last_event_id", value: "17")))

        await client.disconnect()
    }

    @Test("sends the last event id as a header when reconnecting")
    func resumesWithHeader() async throws {
        let first = FakeConnection()
        let second = FakeConnection()
        let transport = FakeTransport(script: [.succeed(first), .succeed(second)])
        let resume = WebSocketClient.ResumeStrategy(placement: .header("Last-Event-ID")) { message in
            if case .text(let text) = message {
                return text
            }
            return nil
        }
        let client = WebSocketClient(url: url, transport: transport, configuration: configuration(resume: resume))
        let (_, recorder) = record(client)
        defer { recorder.cancel() }

        await client.connect()
        #expect(await eventually { await client.state == .connected })
        await first.push(.text("99"))
        #expect(await eventually { await client.lastEventID == "99" })

        await first.drop()
        #expect(await eventually { await transport.calls.count == 2 })
        let secondCall = try #require(await transport.calls.last)
        #expect(secondCall.headers["Last-Event-ID"] == "99")
        #expect(secondCall.headers["Authorization"] == "Bearer token")

        await client.disconnect()
    }

    @Test("queues messages while offline and flushes them in order on connect")
    func flushesQueuedMessages() async throws {
        let connection = FakeConnection()
        let transport = FakeTransport(script: [.succeed(connection)])
        let client = WebSocketClient(url: url, transport: transport, configuration: configuration())

        try await client.send(.text("first"))
        try await client.send(.text("second"))
        await client.connect()

        #expect(await eventually { await connection.sent.count == 2 })
        #expect(await connection.sent == [.text("first"), .text("second")])

        try await client.send(.text("third"))
        #expect(await eventually { await connection.sent.count == 3 })
        #expect(await connection.sent.last == .text("third"))

        await client.disconnect()
    }

    @Test("rejects messages beyond the pending queue limit")
    func rejectsWhenQueueIsFull() async throws {
        let transport = FakeTransport(script: [])
        let client = WebSocketClient(url: url, transport: transport, configuration: configuration(maxPendingMessages: 1))

        try await client.send(.text("fits"))
        await #expect(throws: WebSocketClient.ClientError.pendingQueueFull) {
            try await client.send(.text("does not fit"))
        }
    }

    @Test("a stalled pong is treated as a dead connection and triggers a reconnect")
    func heartbeatTimeoutReconnects() async throws {
        let first = FakeConnection()
        await first.setPingBehavior(.hang)
        let second = FakeConnection()
        let transport = FakeTransport(script: [.succeed(first), .succeed(second)])
        let client = WebSocketClient(
            url: url,
            transport: transport,
            configuration: configuration(heartbeatInterval: .milliseconds(20), heartbeatTimeout: .milliseconds(50))
        )
        let (log, recorder) = record(client)
        defer { recorder.cancel() }

        await client.connect()
        #expect(await eventually { await transport.calls.count == 2 })
        #expect(await eventually { await client.state == .connected })
        #expect(await first.pings >= 1)
        #expect(await first.isClosed)
        #expect(await eventually { await log.failures >= 1 })

        await client.disconnect()
    }

    @Test("pings keep flowing on a healthy connection")
    func heartbeatPings() async throws {
        let connection = FakeConnection()
        let transport = FakeTransport(script: [.succeed(connection)])
        let client = WebSocketClient(
            url: url,
            transport: transport,
            configuration: configuration(heartbeatInterval: .milliseconds(10), heartbeatTimeout: .seconds(1))
        )

        await client.connect()
        #expect(await eventually { await connection.pings >= 3 })
        #expect(await transport.calls.count == 1)
        #expect(await client.state == .connected)

        await client.disconnect()
    }

    @Test("gives up after the policy's maxAttempts and reports it")
    func givesUpAfterRetries() async throws {
        let transport = FakeTransport(script: [.fail(HandshakeFailed()), .fail(HandshakeFailed()), .fail(HandshakeFailed())])
        var policy = ReconnectPolicy.immediate
        policy.maxAttempts = 2
        let client = WebSocketClient(url: url, transport: transport, configuration: configuration(reconnect: policy))
        let (log, recorder) = record(client)
        defer { recorder.cancel() }

        await client.connect()
        #expect(await eventually { await client.state == .closed })
        #expect(await transport.calls.count == 3)
        #expect(await eventually { await log.failures == 3 })
        #expect(await eventually { await log.retriesExhausted })

        await #expect(throws: WebSocketClient.ClientError.closed) {
            try await client.send(.text("too late"))
        }
    }

    @Test("disconnect while waiting to reconnect stops further attempts")
    func disconnectStopsReconnecting() async throws {
        let transport = FakeTransport(script: [.fail(HandshakeFailed())])
        let slow = ReconnectPolicy(initialDelay: .seconds(60), maxDelay: .seconds(60), jitter: 1...1, maxAttempts: nil)
        let client = WebSocketClient(url: url, transport: transport, configuration: configuration(reconnect: slow))

        await client.connect()
        #expect(await eventually { await client.state == .waitingToReconnect(attempt: 1) })

        await client.disconnect()
        #expect(await eventually { await client.state == .closed })
        #expect(await transport.calls.count == 1)
    }
}

# WebSocketClient

[![CI](https://github.com/BogdanGolub/WebSocketClient/actions/workflows/ci.yml/badge.svg)](https://github.com/BogdanGolub/WebSocketClient/actions/workflows/ci.yml)

A small, dependency-free WebSocket client for Swift 6 that survives real networks: reconnects with exponential backoff and jitter, detects dead connections with heartbeats, resumes from the last seen event id, and queues outgoing messages while offline.

The reconnection logic is a plain actor on top of a two-method transport protocol, so every failure mode — dropped sockets, stalled pongs, handshake errors, retries running out — is covered by fast in-memory tests, with no server and no network.

## Features

- **Reconnect with backoff + jitter** — `500 ms → 1 s → 2 s … 30 s`, scaled by a random 50–100 % factor so thousands of clients don't reconnect in the same instant after a server restart. Attempts are capped (10 by default) and the client reports when it gives up.
- **Heartbeat** — a ping every 20 s; if the pong doesn't arrive within 10 s the connection is declared dead and replaced. Half-open TCP connections stop looking "connected" forever.
- **Resume** — the last event id you extract from incoming messages is sent back on reconnect as a query item or header, so the server can replay what was missed instead of the client re-fetching state.
- **Offline queue** — messages sent while disconnected are kept (bounded) and flushed in order as soon as a connection is up.
- **Swift 6, strict concurrency** — the client is an `actor`; everything that crosses it is `Sendable`. No locks, no callbacks.
- **Transport-agnostic** — `URLSessionWebSocketTransport` for Apple platforms is included; anything that can dial a socket can implement `WebSocketTransport`.

## Usage

```swift
import WebSocketClient

let client = WebSocketClient(
    url: URL(string: "wss://chat.example.com/socket")!,
    transport: URLSessionWebSocketTransport(),
    configuration: .init(
        headers: ["Authorization": "Bearer …"],
        reconnect: .default,                 // 500 ms doubling to 30 s, jitter, 10 attempts
        heartbeatInterval: .seconds(20),
        heartbeatTimeout: .seconds(10),
        resume: .init(placement: .queryItem("last_event_id")) { message in
            // Pull the event id out of your protocol's envelope; nil for messages without one.
            guard case .text(let json) = message else { return nil }
            return Envelope(json: json)?.id
        }
    )
)

Task {
    for await event in client.events {
        switch event {
        case .stateChanged(let state):      statusView.update(state)
        case .message(.text(let text)):     handle(text)
        case .message(.data(let data)):     handle(data)
        case .connectionFailed(let error):  log(error)
        case .retriesExhausted:             showOfflineBanner()
        }
    }
}

await client.connect()
try await client.send(.text(#"{"type":"join","room":"42"}"#))
```

`send` never fails because the network is down: while the client is reconnecting the message is queued and delivered later. It throws only when the queue is full (`maxPendingMessages`) or after `disconnect()`.

## How it works

```
idle ──connect()──▶ connecting ──ok──▶ connected ──drop / pong timeout──▶ waitingToReconnect(n)
                        │                                                        │
                        └──────────────── handshake failed ──────────────────────┤
                                                                                 │ backoff(n) · jitter
                                       ◀─────────────────────────────────────────┘
                                                              retries exhausted / disconnect() ──▶ closed
```

- `connect()` starts one long-running task. Each iteration dials the transport, flushes the offline queue, then runs two child tasks side by side: a receive loop and a heartbeat loop. The first one to throw ends the connection; the other is cancelled.
- A heartbeat is a `ping()` raced against `heartbeatTimeout`; whichever finishes first wins, so a stalled pong is indistinguishable from a closed socket — which is the point.
- On reconnect the URL or headers carry the last event id (`ResumeStrategy`), and `attempt` resets to 0 after any successful connection, so one bad minute doesn't shorten the next outage's budget.
- `URLSessionWebSocketTransport` wraps `URLSessionWebSocketTask`. Its `receive()` is cancellation-aware: cancelling the awaiting task cancels the socket, so a dead connection never leaves a receive hanging.

## Testing

`Tests/` drives the client through `FakeTransport` and `FakeConnection`: the test plays the server, pushing messages, dropping the connection or making pings hang, and asserts on reconnect attempts, resume parameters, queue flushing and state transitions. The suite runs in well under a second on macOS and Linux (see [CI](https://github.com/BogdanGolub/WebSocketClient/actions/workflows/ci.yml)).

```
swift test
```

## Requirements

Swift 6.0+ · iOS 17 / macOS 14 / tvOS 17 / watchOS 10 / visionOS 1 · Linux (core client and tests; the `URLSession` transport is Apple-only).

## License

MIT — see [LICENSE](LICENSE).

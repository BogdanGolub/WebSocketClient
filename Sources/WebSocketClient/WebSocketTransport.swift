import Foundation

/// A message exchanged over a WebSocket connection.
public enum WebSocketMessage: Sendable, Equatable {
    case text(String)
    case data(Data)
}

/// One open WebSocket connection.
///
/// Production code wraps `URLSessionWebSocketTask` (see `URLSessionWebSocketTransport`);
/// tests use an in-memory fake that can drop the connection or stall pings on demand.
public protocol WebSocketConnection: Sendable {
    /// Sends a message. Throws if the connection is no longer usable.
    func send(_ message: WebSocketMessage) async throws

    /// Waits for the next incoming message. Throws when the connection closes or fails,
    /// and throws `CancellationError` when the awaiting task is cancelled.
    func receive() async throws -> WebSocketMessage

    /// Sends a ping and returns once the pong arrives. Throws on failure.
    func ping() async throws

    /// Closes the connection. Safe to call more than once.
    func close() async
}

/// Dials a single WebSocket connection.
///
/// A transport only knows how to connect once; reconnection, backoff, heartbeats
/// and resume are the client's job, which keeps them testable without a network.
public protocol WebSocketTransport: Sendable {
    func connect(to url: URL, headers: [String: String]) async throws -> any WebSocketConnection
}

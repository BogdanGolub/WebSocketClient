#if canImport(Darwin)
import Foundation

/// `WebSocketTransport` backed by `URLSessionWebSocketTask`. Apple platforms only.
///
/// `@unchecked Sendable`: `URLSession` is thread-safe by contract but is not marked
/// `Sendable` on every SDK this package supports.
public struct URLSessionWebSocketTransport: WebSocketTransport, @unchecked Sendable {
    private let session: URLSession

    public init(session: URLSession = .shared) {
        self.session = session
    }

    public func connect(to url: URL, headers: [String: String]) async throws -> any WebSocketConnection {
        var request = URLRequest(url: url)
        for (field, value) in headers {
            request.setValue(value, forHTTPHeaderField: field)
        }
        let task = session.webSocketTask(with: request)
        task.resume()
        let connection = URLSessionWebSocketConnection(task: task)
        // URLSession completes the handshake lazily; a first ping surfaces handshake failures
        // here, in `connect`, instead of on the first `receive`.
        do {
            try await connection.ping()
        } catch {
            await connection.close()
            throw error
        }
        return connection
    }
}

/// Wraps one `URLSessionWebSocketTask`. `@unchecked Sendable` because the task is
/// thread-safe by contract but not annotated as `Sendable` on every SDK.
final class URLSessionWebSocketConnection: WebSocketConnection, @unchecked Sendable {
    private let task: URLSessionWebSocketTask

    init(task: URLSessionWebSocketTask) {
        self.task = task
    }

    func send(_ message: WebSocketMessage) async throws {
        switch message {
        case .text(let text):
            try await task.send(.string(text))
        case .data(let data):
            try await task.send(.data(data))
        }
    }

    func receive() async throws -> WebSocketMessage {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<WebSocketMessage, any Error>) in
                task.receive { result in
                    switch result {
                    case .success(.string(let text)):
                        continuation.resume(returning: .text(text))
                    case .success(.data(let data)):
                        continuation.resume(returning: .data(data))
                    case .success:
                        continuation.resume(throwing: URLError(.cannotDecodeContentData))
                    case .failure(let error):
                        continuation.resume(throwing: error)
                    }
                }
            }
        } onCancel: {
            // Unblocks the pending `receive` with an error so the caller can wind down.
            self.cancelTask()
        }
    }

    func ping() async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
            task.sendPing { error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            }
        }
    }

    func close() async {
        task.cancel(with: .normalClosure, reason: nil)
    }

    private func cancelTask() {
        task.cancel(with: .goingAway, reason: nil)
    }
}
#endif

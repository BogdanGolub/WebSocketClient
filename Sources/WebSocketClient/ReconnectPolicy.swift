import Foundation

/// Exponential backoff with jitter.
///
/// The delay before attempt `n` (0-based) is `initialDelay * multiplier^n`, capped at
/// `maxDelay` and then scaled by a random factor drawn from `jitter`. Jitter spreads
/// reconnects of many clients after a server restart instead of letting them all
/// hit the server in the same instant.
public struct ReconnectPolicy: Sendable {
    public var initialDelay: Duration
    public var maxDelay: Duration
    public var multiplier: Double
    /// Range the computed delay is multiplied by. `0.5...1.0` keeps 50–100 % of it.
    public var jitter: ClosedRange<Double>
    /// Consecutive failed attempts after which the client gives up. `nil` retries forever.
    public var maxAttempts: Int?

    public init(
        initialDelay: Duration = .milliseconds(500),
        maxDelay: Duration = .seconds(30),
        multiplier: Double = 2,
        jitter: ClosedRange<Double> = 0.5...1.0,
        maxAttempts: Int? = 10
    ) {
        self.initialDelay = initialDelay
        self.maxDelay = maxDelay
        self.multiplier = multiplier
        self.jitter = jitter
        self.maxAttempts = maxAttempts
    }

    /// 500 ms doubling up to 30 s, 50–100 % jitter, 10 attempts.
    public static let `default` = ReconnectPolicy()

    /// No delay between attempts. Meant for tests.
    public static let immediate = ReconnectPolicy(initialDelay: .zero, maxDelay: .zero, jitter: 1...1, maxAttempts: nil)

    /// Delay before reconnect attempt `attempt` (0-based), or `nil` when retries are exhausted.
    public func delay(forAttempt attempt: Int) -> Duration? {
        var generator = SystemRandomNumberGenerator()
        return delay(forAttempt: attempt, using: &generator)
    }

    /// Same as `delay(forAttempt:)` with an injectable random source.
    public func delay<G: RandomNumberGenerator>(forAttempt attempt: Int, using generator: inout G) -> Duration? {
        if let maxAttempts, attempt >= maxAttempts {
            return nil
        }
        let base = initialDelay.totalSeconds * pow(multiplier, Double(attempt))
        let capped = min(base, maxDelay.totalSeconds)
        let factor = Double.random(in: jitter, using: &generator)
        return .seconds(capped * factor)
    }
}

extension Duration {
    /// The duration as a floating-point number of seconds.
    public var totalSeconds: Double {
        let parts = components
        return Double(parts.seconds) + Double(parts.attoseconds) / 1e18
    }
}

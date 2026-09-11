import Foundation
import Testing
@testable import WebSocketClient

@Suite("ReconnectPolicy")
struct ReconnectPolicyTests {
    private let noJitter = ReconnectPolicy(
        initialDelay: .milliseconds(500),
        maxDelay: .seconds(30),
        multiplier: 2,
        jitter: 1...1,
        maxAttempts: 10
    )

    @Test("delays double from the initial delay")
    func delaysDouble() throws {
        let delays = try (0..<4).map { attempt in
            try #require(noJitter.delay(forAttempt: attempt)).totalSeconds
        }
        #expect(delays == [0.5, 1.0, 2.0, 4.0])
    }

    @Test("delays are capped at maxDelay")
    func delaysAreCapped() throws {
        let delay = try #require(noJitter.delay(forAttempt: 9))
        #expect(delay.totalSeconds == 30)
    }

    @Test("gives up after maxAttempts")
    func givesUpAfterMaxAttempts() {
        #expect(noJitter.delay(forAttempt: 9) != nil)
        #expect(noJitter.delay(forAttempt: 10) == nil)
        #expect(noJitter.delay(forAttempt: 11) == nil)
    }

    @Test("nil maxAttempts retries forever")
    func retriesForever() {
        var forever = noJitter
        forever.maxAttempts = nil
        #expect(forever.delay(forAttempt: 1_000) != nil)
    }

    @Test("jitter keeps the delay within the configured fraction of the base delay")
    func jitterStaysInRange() throws {
        let policy = ReconnectPolicy(initialDelay: .seconds(1), maxDelay: .seconds(30), multiplier: 2, jitter: 0.5...1.0, maxAttempts: nil)
        for attempt in 0..<5 {
            let base = min(pow(2.0, Double(attempt)), 30)
            for _ in 0..<50 {
                let delay = try #require(policy.delay(forAttempt: attempt)).totalSeconds
                #expect(delay >= base * 0.5 - 1e-9)
                #expect(delay <= base + 1e-9)
            }
        }
    }

    @Test("immediate policy has no delay and never gives up")
    func immediatePolicy() throws {
        let delay = try #require(ReconnectPolicy.immediate.delay(forAttempt: 42))
        #expect(delay == .zero)
    }
}

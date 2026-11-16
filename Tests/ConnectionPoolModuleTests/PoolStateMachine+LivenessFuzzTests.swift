@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Testing

/// Liveness under a refusal storm: the server refuses connection attempts for a
/// while (as PostgreSQL does past `max_connections`) and then recovers. Holders
/// always release eventually. Whatever the interleaving, every request must
/// then be served; a request left queued with nothing that can serve it is a
/// hang in production.
@Suite struct PoolStateMachineLivenessFuzzTests {
    struct ConnectionFailed: Error {}

    /// SplitMix64: deterministic across platforms, so a failing seed replays.
    struct SeededGenerator: RandomNumberGenerator {
        var state: UInt64
        mutating func next() -> UInt64 {
            state &+= 0x9E37_79B9_7F4A_7C15
            var z = state
            z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
            z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
            return z ^ (z >> 31)
        }
    }

    enum Step: CustomStringConvertible {
        case request(Int), established(Int), refused(Int), release(Int), timer(String), closed(Int), died(Int), cancelled(Int)
        var description: String {
            switch self {
            case .request(let n): "request \(n)"
            case .established(let id): "connection \(id) established"
            case .refused(let id): "connection \(id) refused"
            case .release(let id): "connection \(id) released"
            case .timer(let what): "timer \(what)"
            case .closed(let id): "connection \(id) closed"
            case .died(let id): "connection \(id) killed by the server"
            case .cancelled(let n): "request \(n) cancelled"
            }
        }
    }

    /// Returns nil when every request was served, else the replayable trace.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    static func run(seed: UInt64, requests total: Int, maximum: Int, minimum: Int, stormAttempts: Int, keepAlive: Bool) -> [Step]? {
        var rng = SeededGenerator(state: seed)
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = minimum
        configuration.maximumConnectionSoftLimit = maximum
        configuration.maximumConnectionHardLimit = maximum
        configuration.keepAliveDuration = keepAlive ? .seconds(10) : nil
        configuration.idleTimeoutDuration = .seconds(30)
        var machine = TestPoolStateMachine(
            configuration: configuration, generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self, clock: MockClock())

        var trace: [Step] = []
        var creations: [TestPoolStateMachine.ConnectionRequest] = []
        var timers: [TestPoolStateMachine.Timer] = []
        var holders: [MockConnection] = []      // one entry per leased stream
        var closing: [MockConnection] = []
        var connections: [Int: MockConnection] = [:]
        var served = 0, issued = 0, attempts = 0, failedRequests = 0, deaths = 0, cancellations = 0
        var queued: [Int: MockRequest<MockConnection>] = [:]   // issued, not yet served/failed
        var numbers: [MockRequest<MockConnection>.ID: Int] = [:]
        var dead = Set<Int>()
        var pinging: [MockConnection] = []

        func cancel(_ tokens: some Sequence<MockTimerCancellationToken>) {
            for token in tokens {
                timers.removeAll { timer in
                    switch token.backing {
                    case .timer(let t): t == timer
                    case .connectionTimer(let t): t == timer.underlying
                    }
                }
            }
        }
        func apply(_ action: TestPoolStateMachine.Action) {
            switch action.request {
            case .leaseConnection(let requests, let connection):
                for request in requests {
                    holders.append(connection); served += 1
                    if let n = numbers[request.id] { queued[n] = nil }
                }
            case .failRequest(let request, let error):
                if let n = numbers[request.id] { queued[n] = nil }
                if error != .requestCancelled { failedRequests += 1 }
            case .failRequests(let requests, _):
                failedRequests += requests.count
                for request in requests { if let n = numbers[request.id] { queued[n] = nil } }
            case .none: break
            }
            switch action.connection {
            case .makeConnection(let request, let cancelled):
                cancel(cancelled); creations.append(request)
            case .makeConnectionsCancelAndScheduleTimers(let requests, let cancelled, let scheduled):
                cancel(cancelled); creations.append(contentsOf: requests); schedule(scheduled)
            case .scheduleTimers(let scheduled): schedule(scheduled)
            case .cancelTimers(let cancelled): cancel(cancelled)
            case .closeConnection(let connection, let cancelled):
                cancel(cancelled); closing.append(connection)
            case .runKeepAlive(let connection, let cancelled):
                if let cancelled { cancel([cancelled]) }
                pinging.append(connection)
            case .initiateShutdown, .cancelEventStreamAndFinalCleanup: Issue.record("pool is not shut down")
            case .none: break
            }
        }
        func schedule(_ scheduled: some Sequence<TestPoolStateMachine.Timer>) {
            for timer in scheduled {
                if let token = machine.timerScheduled(timer, cancelContinuation: MockTimerCancellationToken(timer)) {
                    cancel([token])
                } else {
                    timers.append(timer)
                }
            }
        }

        apply(.init(request: .none, connection: .makeConnectionsCancelAndScheduleTimers(
            .init(machine.refillConnections()), [], [])))

        for _ in 0..<10_000 {
            var choices: [Int] = []
            if issued < total { choices += [0, 0] }
            if !creations.isEmpty { choices += [1, 1] }
            if !holders.isEmpty { choices.append(2) }
            if !timers.isEmpty { choices.append(3) }
            if !closing.isEmpty { choices.append(4) }
            let alive = connections.keys.filter { id in !dead.contains(id) && !closing.contains(where: { Int($0.id) == id }) }.sorted()
            if deaths < 3 && !alive.isEmpty { choices.append(5) }
            // Bounded, so cancelling cannot stand in for serving a stranded request.
            if !queued.isEmpty && cancellations < 2 { choices.append(6) }
            if !pinging.isEmpty { choices.append(7) }
            if choices.isEmpty { break }
            switch choices.randomElement(using: &rng)! {
            case 0:
                issued += 1; trace.append(.request(issued))
                let request = MockRequest(connectionType: MockConnection.self)
                queued[issued] = request; numbers[request.id] = issued
                apply(machine.leaseConnection(request))
            case 1:
                let creation = creations.remove(at: Int.random(in: 0..<creations.count, using: &rng))
                attempts += 1
                if attempts <= stormAttempts && Bool.random(using: &rng) {
                    trace.append(.refused(Int(creation.connectionID)))
                    apply(machine.connectionEstablishFailed(ConnectionFailed(), for: creation))
                } else {
                    trace.append(.established(Int(creation.connectionID)))
                    let connection = MockConnection(id: creation.connectionID)
                    connections[Int(creation.connectionID)] = connection
                    apply(machine.connectionEstablished(connection, maxStreams: 1))
                }
            case 2:
                let connection = holders.remove(at: Int.random(in: 0..<holders.count, using: &rng))
                trace.append(.release(Int(connection.id)))
                apply(machine.releaseConnection(connection, streams: 1))
            case 3:
                let timer = timers.remove(at: Int.random(in: 0..<timers.count, using: &rng))
                trace.append(.timer("\(timer.underlying.usecase) for \(timer.connectionID)"))
                apply(machine.timerTriggered(timer))
            case 5:
                let id = alive.randomElement(using: &rng)!
                deaths += 1; dead.insert(id); trace.append(.died(id))
                holders.removeAll { Int($0.id) == id }
                apply(machine.connectionClosed(connections[id]!))
            case 6:
                let n = queued.keys.sorted().randomElement(using: &rng)!
                cancellations += 1; trace.append(.cancelled(n))
                served += 1 // a cancelled request needs no connection
                apply(machine.cancelRequest(id: queued[n]!.id))
            case 7:
                let connection = pinging.remove(at: Int.random(in: 0..<pinging.count, using: &rng))
                if !dead.contains(Int(connection.id)) {
                    trace.append(.timer("keep-alive done for \(connection.id)"))
                    apply(machine.connectionKeepAliveDone(connection))
                }
            default:
                let connection = closing.removeFirst()
                trace.append(.closed(Int(connection.id)))
                if dead.insert(Int(connection.id)).inserted {
                    apply(machine.connectionClosed(connection))
                }
            }
        }
        return queued.isEmpty && issued == total && failedRequests == 0 ? nil : trace
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test(arguments: [(1, false), (4, false), (20, false), (1, true), (4, true), (20, true)])
    func everyRequestIsServedAfterARefusalStorm(maximum: Int, keepAlive: Bool) {
        var stuck = 0
        var first: (UInt64, [Step])?
        for seed in UInt64(0)..<2_000 {
            if let trace = Self.run(seed: seed, requests: 8, maximum: maximum, minimum: 0, stormAttempts: 12, keepAlive: keepAlive) {
                stuck += 1
                if first == nil { first = (seed, trace) }
            }
        }
        #expect(stuck == 0, "\(stuck)/2000 seeds stranded a request; first seed \(first?.0 ?? 0): \(first?.1.map(\.description).joined(separator: ", ") ?? "")")
    }
}

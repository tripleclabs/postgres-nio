@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Testing

/// Every queued request must stay covered by a connection that is being
/// created, backing off, or will be released. When concurrent creation
/// attempts fail, the pool retries only one of them; the others are
/// destroyed. Unless the pool replaces them, requests beyond the retried one
/// wait for a release that may never come (a caller holding N leases at once
/// deadlocks).
@Suite struct PoolStateMachineQueuedDemandTests {
    struct ConnectionFailed: Error {}

    /// Drives the state machine like `ConnectionPool` does: creates every
    /// connection it asks for (failing the ones listed in `failures` once each,
    /// in creation order), fires every backoff timer, and never releases a
    /// lease. Returns the requests that were leased a connection.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    func leasedRequests(requests: Int, failingAttempts failures: Set<Int>) -> (leased: Set<MockRequest<MockConnection>.ID>, queued: Int) {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 20
        configuration.maximumConnectionHardLimit = 20
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(60)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        var pendingCreations: [TestPoolStateMachine.ConnectionRequest] = []
        var pendingTimers: [TestPoolStateMachine.Timer] = []
        var leased = Set<MockRequest<MockConnection>.ID>()
        var attempts = 0

        func apply(_ action: TestPoolStateMachine.Action) {
            switch action.request {
            case .leaseConnection(let requests, _):
                for request in requests { leased.insert(request.id) }
            case .failRequest(let request, let error):
                Issue.record("request \(request.id) failed: \(error)")
            case .failRequests(let requests, let error):
                Issue.record("\(requests.count) requests failed: \(error)")
            case .none:
                break
            }
            switch action.connection {
            case .makeConnection(let request, _):
                pendingCreations.append(request)
            case .makeConnectionsCancelAndScheduleTimers(let requests, _, let timers):
                pendingCreations.append(contentsOf: requests)
                pendingTimers.append(contentsOf: timers)
            case .scheduleTimers(let timers):
                pendingTimers.append(contentsOf: timers)
            case .runKeepAlive, .cancelTimers, .closeConnection, .initiateShutdown, .cancelEventStreamAndFinalCleanup, .none:
                break
            }
        }

        let mockRequests = (0..<requests).map { _ in MockRequest(connectionType: MockConnection.self) }
        for request in mockRequests { apply(stateMachine.leaseConnection(request)) }

        var steps = 0
        while (!pendingCreations.isEmpty || !pendingTimers.isEmpty) && steps < 1_000 {
            steps += 1
            if !pendingCreations.isEmpty {
                let creation = pendingCreations.removeFirst()
                defer { attempts += 1 }
                if failures.contains(attempts) {
                    apply(stateMachine.connectionEstablishFailed(ConnectionFailed(), for: creation))
                } else {
                    let connection = MockConnection(id: creation.connectionID)
                    apply(stateMachine.connectionEstablished(connection, maxStreams: 1))
                }
            } else {
                let timer = pendingTimers.removeFirst()
                _ = stateMachine.timerScheduled(timer, cancelContinuation: MockTimerCancellationToken(timer))
                apply(stateMachine.timerTriggered(timer))
            }
        }
        #expect(steps < 1_000, "the driver must quiesce")
        return (leased, requests - leased.count)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func oneFailedCreationIsRetriedForItsRequest() {
        let result = leasedRequests(requests: 4, failingAttempts: [0])
        #expect(result.queued == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test(arguments: [
        Set([0, 1]),       // two of four concurrent attempts refused
        Set([0, 1, 2]),    // three refused
        Set([1, 3]),       // non-adjacent
        Set([0, 1, 2, 3]), // all refused once, then the server recovers
    ])
    func concurrentFailedCreationsLeaveNoRequestUncovered(failures: Set<Int>) {
        let result = leasedRequests(requests: 4, failingAttempts: failures)
        #expect(result.queued == 0, "\(result.queued) queued requests have no connection coming")
    }

    /// An idle connection serves a new request even while a concurrent
    /// creation attempt is failing. Queueing the request instead leaves it
    /// waiting on the retry while the idle connection times out.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func idleConnectionServesRequestsWhileCreationIsFailing() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 20
        configuration.maximumConnectionHardLimit = 20
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(60)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // Two concurrent requests: one connection comes up, the other attempt is refused.
        let first = MockRequest(connectionType: MockConnection.self)
        let second = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection(let up, _) = stateMachine.leaseConnection(first).connection,
              case .makeConnection(let refused, _) = stateMachine.leaseConnection(second).connection else {
            Issue.record("expected two connection attempts")
            return
        }
        let connection = MockConnection(id: up.connectionID)
        #expect(stateMachine.connectionEstablished(connection, maxStreams: 1).request
            == .leaseConnection(.init(element: first), connection))
        _ = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: refused)

        // Both holders finish: the established connection serves the second request, then idles.
        #expect(stateMachine.releaseConnection(connection, streams: 1).request
            == .leaseConnection(.init(element: second), connection))
        _ = stateMachine.releaseConnection(connection, streams: 1)

        // A new request must get the idle connection, not wait for the backing-off retry.
        let third = MockRequest(connectionType: MockConnection.self)
        #expect(stateMachine.leaseConnection(third).request == .leaseConnection(.init(element: third), connection))
    }

    /// A request queued at the hard limit waits for that capacity. When the
    /// connection holding it is lost (the server terminated its backend), the
    /// pool must replace it for the request; nothing else will.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func losingTheOnlyConnectionReplacesItForTheQueuedRequest() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(60)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let holder = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection(let first, _) = stateMachine.leaseConnection(holder).connection else {
            Issue.record("expected a connection attempt")
            return
        }
        let connection = MockConnection(id: first.connectionID)
        #expect(stateMachine.connectionEstablished(connection, maxStreams: 1).request
            == .leaseConnection(.init(element: holder), connection))

        // At the hard limit: the next request queues.
        let waiting = MockRequest(connectionType: MockConnection.self)
        #expect(stateMachine.leaseConnection(waiting) == .none())

        // The held connection is lost. Its capacity must go to the waiting request.
        guard case .makeConnection(let replacement, _) = stateMachine.connectionClosed(connection).connection else {
            Issue.record("expected a replacement connection for the queued request")
            return
        }
        let replaced = MockConnection(id: replacement.connectionID)
        #expect(stateMachine.connectionEstablished(replaced, maxStreams: 1).request
            == .leaseConnection(.init(element: waiting), replaced))
    }
}

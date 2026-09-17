// MARK: - Statement metadata on the async query path
//
// `PostgresConnection.query` and `PostgresClient.query` hand back a `PostgresRowSequence`
// and drop the server's `CommandComplete` tag — the one thing that says what the statement
// actually did. Today that tag is only reachable through the `EventLoopFuture` API, as
// `PostgresQueryResult` and `PostgresQueryMetadata`.
//
// Two callers need it:
//
//   * A `COMMIT` issued on an aborted transaction completes with the tag `ROLLBACK` and
//     raises no error at all. Without the tag there is no way to tell a transaction that
//     committed from one whose every write was discarded.
//   * `UPDATE` and `DELETE` report their affected row count in the tag. Without it, a
//     caller that wants the count must append `RETURNING` and count rows instead, making
//     the server materialise a result set nobody reads.
//
// The tag is only valid once the rows have been consumed, so rather than expose a property
// that is wrong to read early, every entry point here consumes the sequence itself and
// returns the metadata afterwards. There is no ordering left for a caller to get wrong,
// and the two shapes mirror the two the `EventLoopFuture` API has always had:
//
//     query(_:logger:)              -> EventLoopFuture<PostgresQueryResult>    collectWithMetadata()
//     query(_:logger:file:line:_:)  -> EventLoopFuture<PostgresQueryMetadata>  consume(onRow:)

extension PostgresRowSequence {
    /// Consume every row of this sequence, passing each to `onRow`, and return the metadata
    /// the server reported for the statement.
    ///
    /// The metadata carries the statement's command tag: the command itself and, for the
    /// commands that report one, the number of rows it affected. A tag is only complete once
    /// the statement has run to its end, which is why this consumes the sequence instead of
    /// offering the metadata alongside it.
    ///
    /// - Note: Like ``collect()`` this consumes the sequence. A ``PostgresRowSequence`` may
    ///         only be iterated once.
    /// - Parameter onRow: A closure that is invoked for every row.
    /// - Returns: The ``PostgresQueryMetadata`` for the statement.
    public func consume(onRow: (PostgresRow) async throws -> ()) async throws -> PostgresQueryMetadata {
        var iterator = self.makeAsyncIterator()
        while let row = try await iterator.next() {
            try await onRow(row)
        }
        return try self.statementMetadata()
    }

    /// Consume and discard every row of this sequence, returning only the metadata the server
    /// reported for the statement.
    ///
    /// This is the shape a write wants. `UPDATE` and `DELETE` report their affected row count
    /// in ``PostgresQueryMetadata/rows``, so a caller no longer has to append `RETURNING` and
    /// count rows to learn it:
    ///
    /// ```swift
    /// let metadata = try await connection.query("DELETE FROM users WHERE id = \(id)", logger: logger).consume()
    /// print(metadata.rows ?? 0)
    /// ```
    ///
    /// It is also the only way to see a `COMMIT` that did not commit. PostgreSQL answers a
    /// `COMMIT` on an aborted transaction with the tag `ROLLBACK`, and raises no error:
    ///
    /// ```swift
    /// let metadata = try await connection.query("COMMIT", logger: logger).consume()
    /// guard metadata.command != "ROLLBACK" else {
    ///     throw MyError.transactionRolledBack // every write in the transaction was discarded
    /// }
    /// ```
    ///
    /// - Note: Like ``collect()`` this consumes the sequence. A ``PostgresRowSequence`` may
    ///         only be iterated once.
    /// - Returns: The ``PostgresQueryMetadata`` for the statement.
    public func consume() async throws -> PostgresQueryMetadata {
        try await self.consume(onRow: { _ in })
    }

    /// Collect every row of this sequence, together with the metadata the server reported for
    /// the statement.
    ///
    /// The async counterpart of `PostgresConnection.query(_:logger:file:line:)`, which returns
    /// the same ``PostgresQueryResult`` as an `EventLoopFuture`.
    ///
    /// - Note: Like ``collect()`` this consumes the sequence. A ``PostgresRowSequence`` may
    ///         only be iterated once.
    /// - Returns: The rows and the ``PostgresQueryMetadata`` for the statement.
    public func collectWithMetadata() async throws -> PostgresQueryResult {
        var rows = [PostgresRow]()
        let metadata = try await self.consume(onRow: { rows.append($0) })
        return PostgresQueryResult(metadata: metadata, rows: rows)
    }

    /// Read the command tag the stream recorded, and parse it as the `EventLoopFuture` API does.
    ///
    /// Only called once the iterator above has run to completion. The rows can only run out
    /// after the server has reported the statement's completion, so the tag is there.
    private func statementMetadata() throws -> PostgresQueryMetadata {
        guard let commandTag = self.rowStream.reportedCommandTag else {
            // Unreachable: consuming the sequence either throws, or ends because the server
            // reported the statement complete, which is where the tag is recorded. Reported
            // rather than trapped, so that being wrong about that cannot crash a caller.
            throw PSQLError.invalidCommandTag("")
        }
        guard let metadata = PostgresQueryMetadata(string: commandTag) else {
            throw PSQLError.invalidCommandTag(commandTag)
        }
        return metadata
    }
}

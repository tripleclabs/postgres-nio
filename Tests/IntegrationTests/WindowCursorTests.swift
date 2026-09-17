import Logging
import NIOCore
import PostgresNIO
import Testing

// MARK: - Regression cover for the fork's own decode path
//
// This fork is maintained as a patch set rebased onto upstream `main`, so every upstream
// pull replays `PostgresRow+Raw.swift` and `PostgresRow+Window.swift` onto code that has
// moved underneath them. Neither has any upstream coverage — they are additions — and both
// are byte level: `WindowCursor` keeps ONE `ByteBuffer` for the whole row and moves its
// reader and writer indices to frame each column in place. A clean rebase proves that still
// compiles. It does not prove the framing is still right, and `makeWindowCursor` is the
// consuming project's production decode path.
//
// The row in `decodesEveryColumn` is shaped deliberately, and the shape is the test. Decoders
// like `String` consume all *readable* bytes, so a window whose writer index is left at the
// end of the row rather than the end of the column makes a string swallow everything after
// it. A fixed width column throws instead, loudly, so it is only a variable width column with
// more columns behind it that returns a wrong answer silently. That is `greeting`: text, in
// the middle, with five columns after it. It is the assertion a future reader is most likely
// to think redundant, and it is the one that fails first when the framing goes.
//
// `trailing` earns its place at the other end: it is reached only after six columns of length
// prefix arithmetic, so drift anywhere upstream surfaces there.
//
// All three of those were verified by mutation, not assumed: dropping the `moveWriterIndex`,
// widening the window to the row end for text columns only, and adding a byte of offset drift
// on the NULL branch each fail this suite.
//
// Kept in a file of its own, rather than folded into `PSQLIntegrationTests`, so that a file
// upstream never touches cannot conflict during a rebase.

@Suite struct WindowCursorTests {
    /// Decode one row of mixed types three ways — window cursor, `PostgresCell`, and the
    /// variadic `row.decode` — and require all three to agree.
    @Test func decodesEveryColumnIdenticallyToTheCellPath() async throws {
        try await withConnection { connection in
            let row = try #require(
                try await connection.query(
                    """
                    SELECT
                        42::int8 AS big,
                        'hello world'::text AS greeting,
                        NULL::text AS absent,
                        '\\xdeadbeef'::bytea AS blob,
                        123.45::float8 AS ratio,
                        true::bool AS flag,
                        'trailing'::text AS trailing
                    """,
                    logger: .psqlTest
                ).collect().first
            )

            // Reference 1: the cell iterator, which is what the window cursor replaces.
            var cells = [PostgresCell]()
            var cellIterator = row.makeIterator()
            while let cell = cellIterator.next() { cells.append(cell) }
            #expect(cells.count == 7)
            let viaCells = (
                try cells[0].decode(Int.self), try cells[1].decode(String.self),
                try cells[2].decode(String?.self), try cells[3].decode(ByteBuffer.self),
                try cells[4].decode(Double.self), try cells[5].decode(Bool.self),
                try cells[6].decode(String.self)
            )

            // Reference 2: the variadic decoder, the path upstream changed in #650.
            let viaDecode = try row.decode(
                (Int, String, String?, ByteBuffer, Double, Bool, String).self, context: .default
            )

            // The window cursor.
            var cursor = row.makeWindowCursor()
            #expect(cursor.lastColumn == nil, "nothing has been decoded yet")
            let big = try cursor.decodeNext(Int.self)
            #expect(cursor.lastColumn?.name == "big")
            #expect(cursor.lastColumn?.dataType == .int8)
            let greeting = try cursor.decodeNext(String.self)
            let absent = try cursor.decodeNext(String?.self)
            let blob = try cursor.decodeNext(ByteBuffer.self)
            #expect(cursor.lastColumn?.name == "blob")
            #expect(cursor.lastColumn?.dataType == .bytea)
            let ratio = try cursor.decodeNext(Double.self)
            let flag = try cursor.decodeNext(Bool.self)
            let trailing = try cursor.decodeNext(String.self)
            #expect(cursor.lastColumn?.name == "trailing")

            #expect(big == viaCells.0 && big == viaDecode.0 && big == 42)
            // The silent one: a mis-framed window leaves this String holding the rest of the
            // row, and every other column still decodes. See the note at the top of the file.
            #expect(
                greeting == viaCells.1 && greeting == viaDecode.1 && greeting == "hello world",
                "the window must end exactly at the column boundary"
            )
            #expect(absent == viaCells.2 && absent == viaDecode.2 && absent == nil)
            #expect(blob == viaCells.3 && blob == viaDecode.3)
            #expect(Array(buffer: blob) == [0xde, 0xad, 0xbe, 0xef])
            #expect(ratio == viaCells.4 && ratio == viaDecode.4 && ratio == 123.45)
            #expect(flag == viaCells.5 && flag == viaDecode.5 && flag == true)

            // Reached only after six columns of length prefix arithmetic, so any drift
            // upstream shows up here.
            #expect(trailing == viaCells.6 && trailing == viaDecode.6 && trailing == "trailing")

            // Running past the last column throws rather than reading whatever follows.
            #expect(throws: (any Error).self) { _ = try cursor.decodeNext(String.self) }
        }
    }

    /// The same agreement, held across a streamed result with NULLs interleaved, so that a
    /// cursor which mis-advances only after a NULL cannot pass.
    @Test func matchesTheCellPathAcrossAStreamedResult() async throws {
        try await withConnection { connection in
            var checked = 0
            var nulls = 0
            for try await row in try await connection.query(
                """
                SELECT i, i::text AS label, CASE WHEN i % 3 = 0 THEN NULL ELSE i::text END AS sometimes
                FROM generate_series(1, 500) AS i
                """,
                logger: .psqlTest
            ) {
                let reference = try row.decode((Int, String, String?).self, context: .default)
                var cursor = row.makeWindowCursor()
                #expect(try cursor.decodeNext(Int.self) == reference.0)
                #expect(try cursor.decodeNext(String.self) == reference.1)
                let sometimes = try cursor.decodeNext(String?.self)
                #expect(sometimes == reference.2)
                #expect(cursor.lastColumn?.name == "sometimes")
                if sometimes == nil { nulls += 1 }
                checked += 1
            }
            #expect(checked == 500)
            #expect(nulls == 166, "the result must actually exercise the NULL branch")
        }
    }

    /// A decode that cannot succeed must still throw, not return something wrong. The cursor
    /// hands the framed window to `_decodeRaw`, so this is what proves it frames rather than
    /// bypasses the decoder.
    @Test func reportsATypeMismatchRatherThanDecodingGarbage() async throws {
        try await withConnection { connection in
            let row = try #require(
                try await connection.query("SELECT 'not a number'::text AS oops", logger: .psqlTest)
                    .collect().first
            )
            var cursor = row.makeWindowCursor()
            #expect(throws: (any Error).self) { _ = try cursor.decodeNext(Int.self) }
        }
    }

    /// `makeRawIterator` yields the same columns as the cell iterator, minus the name.
    @Test func rawIteratorMatchesTheCellIterator() async throws {
        try await withConnection { connection in
            let row = try #require(
                try await connection.query(
                    "SELECT 1::int8 AS a, NULL::text AS b, 'z'::text AS c", logger: .psqlTest
                ).collect().first
            )

            var cells = [PostgresCell]()
            var cellIterator = row.makeIterator()
            while let cell = cellIterator.next() { cells.append(cell) }

            var raw = [PostgresRow.RawCell]()
            var rawIterator = row.makeRawIterator()
            while let cell = rawIterator.next() { raw.append(cell) }

            #expect(raw.count == cells.count)
            #expect(raw.count == 3)
            for (rawCell, cell) in zip(raw, cells) {
                #expect(rawCell.bytes == cell.bytes)
                #expect(rawCell.dataType == cell.dataType)
                #expect(rawCell.format == cell.format)
            }
            #expect(raw[1].bytes == nil, "SQL NULL")
        }
    }
}

import NIOCore

// MARK: - Experimental low-overhead cell iteration
//
// `PostgresRow.Iterator` materialises a `PostgresCell` per column, which copies
// `column.name` into the cell. That String is a retain/release pair per cell —
// 21,000 of them for a 21-column, 1,000-row result — and an ordinal decoder
// never reads it.
//
// This iterator yields only what binary decoding needs. It exists to attribute
// cost; the shape of any upstream proposal depends on what it measures.

extension PostgresRow {
    /// A column's payload, without the column's name.
    public struct RawCell: Sendable {
        public var bytes: ByteBuffer?
        public var dataType: PostgresDataType
        public var format: PostgresFormat

        @inlinable
        public init(bytes: ByteBuffer?, dataType: PostgresDataType, format: PostgresFormat) {
            self.bytes = bytes
            self.dataType = dataType
            self.format = format
        }
    }

    public struct RawIterator: IteratorProtocol {
        public typealias Element = RawCell

        @usableFromInline
        var columnIndex: Int
        @usableFromInline
        let columns: [RowDescription.Column]
        @usableFromInline
        var dataIterator: DataRow.Iterator

        @usableFromInline
        init(_ row: PostgresRow) {
            self.columnIndex = 0
            self.columns = row.columns
            self.dataIterator = row.data.makeIterator()
        }

        @inlinable
        public mutating func next() -> RawCell? {
            guard let bytes = self.dataIterator.next() else {
                return nil
            }
            let column = self.columns[self.columnIndex]
            self.columnIndex += 1
            // `column.dataType` and `column.format` are trivial values; the
            // String `column.name` is deliberately not touched.
            return RawCell(bytes: bytes, dataType: column.dataType, format: column.format)
        }
    }

    /// Iterate this row's columns without materialising column names.
    @inlinable
    public func makeRawIterator() -> RawIterator {
        RawIterator(self)
    }
}

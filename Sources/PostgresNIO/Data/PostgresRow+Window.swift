import NIOCore

// MARK: - Experimental zero-slice column cursor
//
// `DataRow.subscript` returns `bytes.getSlice(...)` — a fresh `ByteBuffer`
// sharing storage, so every column costs a retain/release pair on top of the
// one `PostgresCell` already pays for the column name. Measured against Rust's
// tokio-postgres, that per-cell ARC traffic is ~24ns/cell and accounts for
// essentially all of the remaining decode gap.
//
// This cursor keeps ONE `ByteBuffer` for the whole row and moves its reader and
// writer indices to frame each column in place. Framing is required, not
// cosmetic: decoders like `String` consume all *readable* bytes, so the window
// must end exactly at the column boundary.
//
// One retain per row instead of one per column.

extension PostgresRow {
    public struct WindowCursor {
        /// The row's bytes. Indices are moved per column; the struct is never
        /// copied, so its storage reference is retained once for the whole row.
        @usableFromInline
        var window: ByteBuffer?

        /// Always nil. Handed to `_decodeRaw` for SQL NULL columns.
        @usableFromInline
        var nullWindow: ByteBuffer?

        @usableFromInline
        var offset: Int
        @usableFromInline
        let rowEnd: Int
        @usableFromInline
        var columnIndex: Int
        @usableFromInline
        let columns: [RowDescription.Column]

        @usableFromInline
        init(_ row: PostgresRow) {
            let bytes = row.data.bytes
            self.window = bytes
            self.nullWindow = nil
            self.offset = bytes.readerIndex
            self.rowEnd = bytes.readerIndex + bytes.readableBytes
            self.columnIndex = 0
            self.columns = row.columns
        }

        @inlinable
        public mutating func decodeNext<T: PostgresDecodable>(
            _ type: T.Type
        ) throws -> T {
            guard self.columnIndex < self.columns.count else {
                throw PostgresDecodingError.Code.missingData
            }
            let column = self.columns[self.columnIndex]
            self.columnIndex += 1

            // The length prefix lives past the previous column's window, so
            // open the buffer back up to the full row before reading it.
            self.window!.moveWriterIndex(to: self.rowEnd)
            guard let length = self.window!.getInteger(at: self.offset, as: Int32.self) else {
                throw PostgresDecodingError.Code.missingData
            }
            self.offset += MemoryLayout<Int32>.size

            if length < 0 {
                // SQL NULL. `_decodeRaw` decides whether that is acceptable for T.
                return try T._decodeRaw(
                    from: &self.nullWindow,
                    type: column.dataType,
                    format: column.format,
                    context: .default
                )
            }

            let end = self.offset + Int(length)
            guard end <= self.rowEnd else {
                throw PostgresDecodingError.Code.missingData
            }
            self.window!.moveReaderIndex(to: self.offset)
            self.window!.moveWriterIndex(to: end)
            self.offset = end

            return try T._decodeRaw(
                from: &self.window,
                type: column.dataType,
                format: column.format,
                context: .default
            )
        }

        /// Metadata for the column most recently returned by `decodeNext`.
        ///
        /// Reading a column's name is what `PostgresCell` does eagerly, and
        /// avoiding it per cell is most of this cursor's advantage. Exposing it
        /// here lets a caller name the column **only when throwing**, so
        /// diagnostics cost nothing on the success path.
        @inlinable
        public var lastColumn: (name: String, dataType: PostgresDataType)? {
            guard self.columnIndex > 0, self.columnIndex <= self.columns.count else {
                return nil
            }
            let column = self.columns[self.columnIndex - 1]
            return (column.name, column.dataType)
        }
    }

    /// A cursor that decodes columns in order without allocating a `ByteBuffer`
    /// slice per column.
    @inlinable
    public func makeWindowCursor() -> WindowCursor {
        WindowCursor(self)
    }
}

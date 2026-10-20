@_spi(ConnectionPool) import PostgresNIO
import Testing
import NIOPosix
import NIOSSL
import Logging
import Atomics
import Foundation

@Suite(.serialized)
struct PostgresClientTests {

    /// A connection whose backend died can be handed back to the pool before
    /// the pool sees the close. A pool query written to it must fail rather than
    /// strand its promise: a stranded promise hangs the caller forever and never
    /// releases the lease, so the pool loses that connection slot for good.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test(.timeLimit(.minutes(1)))
    func poolQueryAfterBackendTerminationNeverHangs() async throws {
        var config = PostgresClient.Configuration.makeTestConfiguration()
        config.options.maximumConnections = 1
        let client = PostgresClient(
            configuration: config, eventLoopGroup: MultiThreadedEventLoopGroup.singleton,
            backgroundLogger: Logger(label: "test")
        )
        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask { await client.run() }
            for _ in 0..<200 {
                // The backend kills itself; the error reaches us before the
                // channel close does, and the lease goes back to the pool.
                _ = try? await client.withConnection { connection in
                    try await connection.query("SELECT pg_terminate_backend(pg_backend_pid())", logger: Logger(label: "test"))
                }
                // Either answers or fails. Hanging is the bug.
                _ = try? await client.query("SELECT 1")
            }
            taskGroup.cancelAll()
        }
    }

    /// A lessee that saw its backend die mid-query releases a lease the pool
    /// must not hand to the next borrower: the following queries have to run on
    /// a new connection rather than fail on the dead one.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test(.timeLimit(.minutes(1)))
    func poolNeverLeasesAConnectionWhoseQueryFailedOnBackendTermination() async throws {
        var config = PostgresClient.Configuration.makeTestConfiguration()
        config.options.maximumConnections = 1
        let client = PostgresClient(
            configuration: config, eventLoopGroup: MultiThreadedEventLoopGroup.singleton,
            backgroundLogger: Logger(label: "test")
        )
        let logger = Logger(label: "test")
        let killer = try await PostgresConnection.test(on: MultiThreadedEventLoopGroup.singleton.any()).get()
        defer { try? killer.close().wait() }
        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask { await client.run() }
            for _ in 0..<50 {
                await #expect(throws: PSQLError.self) {
                    try await client.withConnection { connection in
                        let rows = try await connection.query("SELECT pg_backend_pid()", logger: logger)
                        var pid: Int32 = 0
                        for try await row in rows.decode(Int32.self) { pid = row }
                        async let killed = killer.query("SELECT pg_terminate_backend(\(pid))", logger: logger)
                        // Dies mid-statement: the error reaches this lessee before
                        // the lease returns to the pool.
                        for try await _ in try await connection.query("SELECT pg_sleep(30)", logger: logger) {}
                        _ = try await killed
                    }
                }
                try await client.query("SELECT 1")
                try await client.withConnection { connection in
                    try await connection.query("SELECT 1", logger: logger)
                }
            }
            taskGroup.cancelAll()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func getConnection() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            let iterations = 1000

            for _ in 0..<iterations {
                taskGroup.addTask {
                    try await client.withConnection() { connection in
                        _ = try await connection.query("SELECT 1", logger: logger)
                    }
                }
            }

            for _ in 0..<iterations {
                try await taskGroup.next()
            }

            taskGroup.cancelAll()
        }
    }
    
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func transaction() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        
        let tableName = "test_client_transactions"
        
        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )
        
        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }
            
            try await client.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: tableName)" (
                    id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    uuid UUID NOT NULL
                );
                """,
                logger: logger
            )
            
            let iterations = 1000
            
            for _ in 0..<iterations {
                taskGroup.addTask {
                    let _ = try await client.withTransaction(logger: logger) { transaction in
                        try await transaction.query(
                        """
                        INSERT INTO "\(unescaped: tableName)" (uuid) VALUES (\(UUID()));
                        """,
                        logger: logger
                        )
                    }
                }
            }
            
            for _ in 0..<iterations {
                try await taskGroup.next()
            }
            
            let rows = try await client.query(#"SELECT COUNT(1)::INT AS table_size FROM "\#(unescaped: tableName)";"#, logger: logger).decode(Int.self)
            for try await (count) in rows {
                #expect(count == iterations)
            }
            
            /// Test roll back
            taskGroup.addTask {
                let error = try await #require(throws: PostgresTransactionError.self) {
                    let _ = try await client.withTransaction(logger: logger) { transaction in
                        /// insert valid data
                        try await transaction.query(
                            """
                            INSERT INTO "\(unescaped: tableName)" (uuid) VALUES (\(UUID()));
                            """,
                            logger: logger
                        )
                        
                        /// insert invalid data
                        try await transaction.query(
                            """
                            INSERT INTO "\(unescaped: tableName)" (uuid) VALUES (\(iterations));
                            """,
                            logger: logger
                        )
                    }
                }

                #expect((error.closureError as? PSQLError)?.code == .server)
                #expect((error.closureError as? PSQLError)?.serverInfo?[.severity] == "ERROR")
            }
            try await taskGroup.next()
            
            let row = try await client.query(#"SELECT COUNT(1)::INT AS table_size FROM "\#(unescaped: tableName)";"#, logger: logger).decode(Int.self)
            
            for try await (count) in row {
                #expect(count == iterations)
            }
            
            try await client.query(
                """
                DROP TABLE "\(unescaped: tableName)";
                """,
                logger: logger
            )
            
            taskGroup.cancelAll()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func applicationNameIsForwardedCorrectly() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger

        var clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let applicationName = "postgres_nio_test_run"
        clientConfig.options.additionalStartupParameters = [("application_name", applicationName)]
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            let rows = try await client.query("select * from pg_stat_activity;");
            var applicationNameFound = 0
            for try await row in rows {
                let randomAccessRow = row.makeRandomAccess()
                if try randomAccessRow["application_name"].decode(String?.self) == applicationName {
                    applicationNameFound += 1
                }
            }

            #expect(applicationNameFound >= 1)

            taskGroup.cancelAll()
        }
    }


    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func queryDirectly() async throws {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            for _ in 0..<10000 {
                taskGroup.addTask {
                    await #expect(throws: Never.self) { 
                        try await client.query("SELECT 1", logger: logger) 
                    }
                }
            }

            for _ in 0..<10000 {
                try await taskGroup.next()
            }

            taskGroup.cancelAll()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func queryTable() async throws {
        let tableName = "test_client_prepared_statement"

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            try await client.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: tableName)" (
                    id SERIAL PRIMARY KEY,
                    uuid UUID NOT NULL
                );
                """,
                logger: logger
            )

            for _ in 0..<1000 {
                try await client.query(
                    """
                    INSERT INTO "\(unescaped: tableName)" (uuid) VALUES (\(UUID()));
                    """,
                    logger: logger
                )
            }

            let rows = try await client.query(#"SELECT id, uuid FROM "\#(unescaped: tableName)";"#, logger: logger).decode((Int, UUID).self)
            for try await (id, uuid) in rows {
                logger.info("id: \(id), uuid: \(uuid.uuidString)")
            }

            struct Example: PostgresPreparedStatement {
                static let sql = "SELECT id, uuid FROM test_client_prepared_statement WHERE id < $1"
                typealias Row = (Int, UUID)
                var id: Int
                func makeBindings() -> PostgresBindings {
                    var bindings = PostgresBindings()
                    bindings.append(self.id)
                    return bindings
                }
                func decodeRow(_ row: PostgresNIO.PostgresRow) throws -> Row {
                    try row.decode(Row.self)
                }
            }

            for try await (id, uuid) in try await client.execute(Example(id: 200), logger: logger) {
                logger.info("id: \(id), uuid: \(uuid.uuidString)")
            }

            try await client.query(
                """
                DROP TABLE "\(unescaped: tableName)";
                """,
                logger: logger
            )

            taskGroup.cancelAll()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func lTree() async throws {
        let tableName = "test_client_ltree"

        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger

        let clientConfig = PostgresClient.Configuration.makeTestConfiguration()
        let client = PostgresClient(
            configuration: clientConfig, eventLoopGroup: MultiThreadedEventLoopGroup.singleton, backgroundLogger: logger
        )

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await client.run()
            }

            try await client.query("CREATE EXTENSION IF NOT EXISTS ltree;")

            try await client.query("DROP TABLE IF EXISTS \"\(unescaped: tableName)\";")

            try await client.query(
                """
                CREATE TABLE IF NOT EXISTS "\(unescaped: tableName)" (
                    id SERIAL PRIMARY KEY,
                    label ltree NOT NULL
                );
                """
            )

            try await client.query(
                """
                INSERT INTO "\(unescaped: tableName)" (label) VALUES ('foo.bar.baz')
                """
            )

            let rows = try await client.query(
                """
                SELECT id, label FROM "\(unescaped: tableName)" WHERE label ~ 'foo.*'
                """
            )

            var count = 0
            for try await _ in rows.decode((Int, String).self) {
                count += 1
            }
            #expect(count == 1)

            taskGroup.cancelAll()
        }
    }

}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PostgresClient.Configuration {
    static func makeTestConfiguration() -> PostgresClient.Configuration {
        var tlsConfiguration = TLSConfiguration.makeClientConfiguration()
        tlsConfiguration.certificateVerification = .none
        var clientConfig = PostgresClient.Configuration(
            host: env("POSTGRES_HOSTNAME") ?? "localhost",
            port: env("POSTGRES_PORT").flatMap({ Int($0) }) ?? 5432,
            username: env("POSTGRES_USER") ?? "test_username",
            password: env("POSTGRES_PASSWORD") ?? "test_password",
            database: env("POSTGRES_DB") ?? "test_database",
            tls: .prefer(tlsConfiguration)
        )
        clientConfig.options.minimumConnections = 0
        clientConfig.options.maximumConnections = 12*4
        clientConfig.options.keepAliveBehavior = .init(frequency: .seconds(5))
        clientConfig.options.connectionIdleTimeout = .seconds(15)

        return clientConfig
    }
}

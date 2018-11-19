package com.synappticlabs.db.sqlite3

import kotlin.test.*

class ResultSetTests {
    lateinit var db: Database

    fun `Reads column names from a splat query`() {
        val result = db.query("select * from foo")
        val resultSet = assertResultSet(result)

        assertEquals(3, resultSet.columnCount)
        assertEquals("bar", resultSet.nameOf(0))
        assertEquals(1, resultSet.indexOf("baz"))
    }

    internal fun assertResultSet(result: SQLiteResult): ResultSet {
        when {
            result is SQLiteResult.QueryResult -> return result.resultSet
            result is SQLiteResult.FailureResult -> fail("Sqlite operation failed with $result.message")
            else -> fail("Incorrect SQLiteResult type returned.")
        }
    }

    @BeforeEach fun setup() {
        db = Database.open()

        db.execute("""
        create table foo(
            id integer primary key autoincrement,
            bar text not null,
            baz text not null,
            num integer
        );
        """)

        val data = listOf(
            hashMapOf("bar" to "bara", "baz" to "bazb", "num" to 1),
            hashMapOf("bar" to "barb", "baz" to "bazc", "num" to 2),
            hashMapOf("bar" to "barc", "baz" to "bazd", "num" to 3),
            hashMapOf("bar" to "bard", "baz" to "baze", "num" to 4)
        )
        data.forEach { args ->
            db.execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                parameters.bind(args)
            }
        }
    }

    @AfterEach fun cleanup() {
        db.close()
    }
}
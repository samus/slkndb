package com.synappticlabs.db.sqlite3

import kotlinx.cinterop.*
import kotlin.test.*

class ResultSetTests {
    lateinit var db: Database
    var resultSet: ResultSet? = null

    @Test
    fun `Will not die on a bad sql statement`() {
        val resultSet = getResultSet("select num from foo where num = null") //Should be is instead of =
        assertFalse(resultSet.next())
    }

    @Test
    fun `Reads column names from a splat query`() {
        val resultSet = getResultSet("select * from foo")

        assertEquals(4, resultSet.columnCount)
        assertEquals("bar", resultSet.nameOf(1))
        assertEquals(2, resultSet.indexOf("baz"))
    }

    @Test
    fun `Reads ints from a result set`() {
        val resultSet = getResultSet("select num from foo where num = 1")
        assertTrue(resultSet.next())
        val num: Int? = resultSet.int("num")
        assertNotNull(num)
        assertEquals(1, num)
    }

    @Test
    fun `Reads null ints from a result set`() {
        val resultSet = getResultSet("select num from foo where bar = \"bard\"")

        assertTrue(resultSet.next())
        val num: Int? = resultSet.int("num")
        assertNull(num)
    }

    @Test
    fun `Reads strings from a result set`() {
        val resultSet = getResultSet("select bar, baz from foo where num = 1")

        assertTrue(resultSet.next())
        val bar: String? = resultSet.string("bar")
        assertEquals("bara", bar)
    }

    @Test
    fun `Reads two rows from a result set`() {
        val resultSet = getResultSet("select bar, baz from foo")

        assertTrue(resultSet.next())
        assertTrue(resultSet.next())
    }

    @Test
    fun `Reads all rows from a query`() {
        val resultSet = getResultSet("select bar, baz from foo")
        var count = 0
        try {
            while (resultSet.next()) {
                count += 1
            }
        } catch (err: SQLiteError) {
            fail(err.message)
        }

        assertEquals(4, count, "Iteration through full result set failed.")
    }

    @Test
    fun `Handles empty result sets gracefully`() {
        val resultSet = getResultSet("select bar, baz from foo where num = 1000")
        assertFalse(resultSet.next())
    }

    @Test
    fun `Writes and reads a blob`() {
        db.execute("""
            create table bucket(
                id integer primary key autoincrement,
                data blob
            );
        """.trimIndent())
        val expected = byteArrayOf(1,2,3,4)
        db.execute("insert into bucket(data) values (?);") {
            parameters.bind(1, expected)
        }
        val resultSet = getResultSet("select * from bucket")
        assertTrue(resultSet.next())
        val actual = resultSet.byteArray("data")
        assertNotNull(actual)
        if (actual != null) {
            assertEquals(expected.size, actual.size)
            for (i in 0 until expected.size) {
                assertEquals(expected[i], actual[i], "Byte $i was in actual did not match expected.")
            }
        }
    }

    fun getResultSet(query: String): ResultSet {
        val resultSet = db.query(query)
        this.resultSet = resultSet

        return resultSet
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
            hashMapOf("bar" to "bard", "baz" to "baze", "num" to null)
        )
        data.forEach { args ->
            db.execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                parameters.bind(args)
            }
        }
    }

    @AfterEach fun cleanup() {
        resultSet?.close()
        db.close()
    }
}
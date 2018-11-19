package com.synappticlabs.db.sqlite3

import kotlin.test.*

class DatabaseTests {
    @Test
    fun `Can return the version string`() {
        assertNotEquals("", Database.sqliteVersion, "version mismatch")
    }

    @Test
    fun `Opens and closes a database`() {
        val db = Database.open()
        assertTrue(db.opened)
        assertTrue(db.close())
        assertFalse(db.opened)
    }
}

class DatabaseExecutionTests {
    lateinit var db: Database
    val createTable = """
        create table foo(
            id integer primary key autoincrement,
            bar text not null,
            baz text not null,
            num integer
        );
        """
    var insertFooPosArgs = "insert into foo(bar, baz, num) values (?,?,?);"


    @Test
    fun `Can execute a dml statement`() {
        assertTrue(db.execute(createTable))
    }

    @Test
    fun `Inserts a row into a table`() {
        db.execute(createTable)
        var insertFoo = "insert into foo(bar, baz) values (\"a\",\"b\");"
        assertTrue(db.execute(insertFoo))
    }

    @Test
    fun `Inserts a row into a table using positional arguments`() {
        db.execute(createTable)
        val success = db.execute(insertFooPosArgs) {
            assertEquals(3, parameters.count)
            parameters.bind(1, "a")
            parameters.bind(2, "b")
            parameters.bind(3, null)
        }
        assertTrue(success)
    }

    @Test
    fun `Inserts a row into a table using an array as positional arguments`() {
        db.execute(createTable)
        val args = listOf("a", "b", 1)
        assertTrue(db.execute(insertFooPosArgs){
            parameters.bind(args)
        })
    }

    @Test
    fun `Inserts a row into a table using a map as named arguments`() {
        db.execute(createTable)
        val args = hashMapOf("bar" to "a", "baz" to "b", "num1" to 5)
        assertTrue(db.execute("insert into foo(bar, baz, num) values (:bar,:baz,:num1);") {
            parameters.bind(args)
        })
    }

    @BeforeEach fun setup() {
        db = Database.open()
    }

    @AfterEach fun cleanup() {
        db.close()
    }
}
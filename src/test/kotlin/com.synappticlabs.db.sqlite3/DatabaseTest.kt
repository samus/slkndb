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


    @Test
    fun `Can open a read only database`() {
        val db = Database.open("readOnly.db")
        assertTrue(db.opened, "Could not create db for read only test.")
        assertTrue(db.close())
        val ro = Database.open("readOnly", readOnly = true)
        assertTrue(ro.opened)
    }

    @Test
    fun `Opens a database once in rw mode and once in r mode`() {
        val name = "DatabaseTests.db"
        try {
            val dbrw = Database.open(name)
            assertTrue(dbrw.opened, "Unable to open rw database")
            val dbro = Database.open(name, readOnly = true)
            assertTrue(dbro.opened, "Unable to open ro database")
        } catch (ex: Exception) {
            fail(ex.message)
        }
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
        val result = db.execute(createTable)
        assertSuccessResult(result, 0, false)
    }

    @Test
    fun `Inserts a row into a table`() {
        db.execute(createTable)
        var insertFoo = "insert into foo(bar, baz) values (\"a\",\"b\");"
        val result = db.execute(insertFoo)
        assertSuccessResult(result, 1, true)

    }

    @Test
    fun `Inserts a row into a table using positional arguments`() {
        db.execute(createTable)
        val result = db.execute(insertFooPosArgs) {
            assertEquals(3, parameters.count)
            parameters.bind(1, "a")
            parameters.bind(2, "b")
            parameters.bind(3, null)
        }
        assertTrue(result.success)
    }

    @Test
    fun `Inserts a row into a table using an array as positional arguments`() {
        db.execute(createTable)
        val args = listOf("a", "b", 1)
        val result = db.execute(insertFooPosArgs){
            parameters.bind(args)
        }
        assertSuccessResult(result, 1, true)
    }

    @Test
    fun `Inserts a row into a table using a map as named arguments`() {
        db.execute(createTable)
        val args = hashMapOf("bar" to "a", "baz" to "b", "num1" to 5)
        val result = db.execute("insert into foo(bar, baz, num) values (:bar,:baz,:num1);") {
            parameters.bind(args)
        }
        assertSuccessResult(result, 1, true)
    }

    @Test
    fun `Deletes rows from a table`() {
        setupData()
        val result = db.execute("delete from foo where num > 2")
        assertSuccessResult(result, 2, false)
    }

    @Test
    fun `Deletes no rows from a table when the criteria does not match`() {
        setupData()
        val result = db.execute("delete from foo where num > 1000")
        assertSuccessResult(result, 0, false)
    }

    fun assertSuccessResult(result: SQLiteResult, modificationCount: Int, checkLastRowId: Boolean) {
        when (result) {
            is SQLiteResult.ModificationResult -> {
                assertEquals(modificationCount, result.modificationCount)
                if (checkLastRowId) { assertNotEquals(0, result.lastRowInsertId) }
            }
            is SQLiteResult.FailureResult -> fail("Sqlite operation failed with $result.message")
        }
    }

    fun setupData(){
        db.execute(createTable)
        val data = listOf(
            hashMapOf("bar" to "a", "baz" to "b", "num1" to 1),
            hashMapOf("bar" to "b", "baz" to "c", "num1" to 2),
            hashMapOf("bar" to "c", "baz" to "d", "num1" to 3),
            hashMapOf("bar" to "d", "baz" to "e", "num1" to 4)
            )
        data.forEach { args ->
            db.execute("insert into foo(bar, baz, num) values (:bar,:baz,:num1);") {
                parameters.bind(args)
            }
        }
    }

    @BeforeEach fun setup() {
        db = Database.open()
    }

    @AfterEach fun cleanup() {
        db.close()
    }
}
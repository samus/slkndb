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
            bar text not null
        );
        """

    @Test
    fun `Can execute a dml statement`() {
        assertTrue(db.execute(createTable))
    }

    @BeforeEach fun setup() {
        db = Database.open()
    }

    @AfterEach fun cleanup() {
        db.close()
    }
}
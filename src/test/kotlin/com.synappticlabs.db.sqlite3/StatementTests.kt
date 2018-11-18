package com.synappticlabs.db.sqlite3

import kotlin.test.*

class StatementTests {
    lateinit var db: Database

    @Test
    fun `Can prepare a statment`() {
        val st = Statement.prepare("select 1;", db.conn)
        assertNotNull(st)
        st.close()
    }

    @Test
    fun `Will not prepare a bad statement`() {
        assertFailsWith(SQLiteError::class) {
            Statement.prepare("bad sql", db.conn)
        }
    }

    @BeforeEach fun setup() {
        db = Database.open()
    }

    @AfterEach fun cleanup() {
        db.close()
    }
}
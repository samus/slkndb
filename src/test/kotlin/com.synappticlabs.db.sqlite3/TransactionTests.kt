package com.synappticlabs.db.sqlite3

import kotlin.test.*

class TransactionTests {
    lateinit var db: Database

    @Test
    fun `Commits a transaction with no modifications`() {
        db.transaction() {
            return@transaction TransactionResult.COMMIT
        }
    }

    @Test
    fun `Commits a transaction with one modification`() {
        db.transaction() {
            execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                parameters.bind(hashMapOf("bar" to "bare", "baz" to "bazf", "num" to 5))
            }
            return@transaction TransactionResult.COMMIT
        }
        val resultSet = db.query("select * from foo where num = 5")
        assertTrue(resultSet.next())
        resultSet.close()
    }

    @Test
    fun `Rollsback a transaction with one modification`() {
        db.transaction() {
            execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                parameters.bind(hashMapOf("bar" to "bare", "baz" to "bazf", "num" to 5))
            }
            return@transaction TransactionResult.ROLLBACK
        }
        val resultSet = db.query("select * from foo where num = 5")
        assertFalse(resultSet.next())
        resultSet.close()
    }

    @Test
    fun `Queries data and updates a row in a transaction`() {
        db.transaction() {
            try {
                val resultSet = query("select * from foo where num is null")
                resultSet.next()
                val id = resultSet.int("id")
                val num = 5
                resultSet.close()

                execute("update foo set num = :num where id = :id;") {
                    parameters.bind(hashMapOf("num" to num, "id" to id))
                }
            } catch (ex: Exception) {
                fail("Test failed with exception: $ex")
            }
            return@transaction TransactionResult.COMMIT
        }
        val resultSet = db.query("select * from foo where num = 5")
        assertTrue(resultSet.next())
        resultSet.close()
    }

    @Test
    fun `Can nest transactions`() {
        db.transaction() {
            try {
                val resultSet = query("select * from foo where num is null")
                resultSet.next()
                val id = resultSet.int("id")
                val num = 5
                resultSet.close()
                execute("update foo set num = :num where id = :id;") {
                    parameters.bind(hashMapOf("num" to num, "id" to id))
                }
                nest("a") {
                    execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                        parameters.bind(hashMapOf("bar" to "bare", "baz" to "bazf", "num" to 6))
                    }
                    execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                        parameters.bind(hashMapOf("bar" to "barf", "baz" to "bazg", "num" to 7))
                    }
                    return@nest TransactionResult.COMMIT
                }
            } catch (ex: Exception) {
                fail("Test failed with exception: $ex")
            }
            return@transaction TransactionResult.COMMIT
        }
        val resultSet = db.query("select * from foo where num >= 5")
        var rowCount = 0
        while (resultSet.next()) {
            rowCount += 1
        }
        assertEquals(3, rowCount, "Inner transaction did nothing")
    }

    @Test
    fun `Can rollback save points`(){
        db.transaction() {
            try {
                val resultSet = query("select * from foo where num is null")
                resultSet.next()
                val id = resultSet.int("id")
                val num = 5
                resultSet.close()
                execute("update foo set num = :num where id = :id;") {
                    parameters.bind(hashMapOf("num" to num, "id" to id))
                }
                nest("a") {
                    execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                        parameters.bind(hashMapOf("bar" to "bare", "baz" to "bazf", "num" to 6))
                    }
                    execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                        parameters.bind(hashMapOf("bar" to "barf", "baz" to "bazg", "num" to 7))
                    }
                    return@nest TransactionResult.ROLLBACK
                }
            } catch (ex: Exception) {
                fail("Test failed with exception: $ex")
            }
            return@transaction TransactionResult.COMMIT
        }
        val resultSet = db.query("select * from foo where num >= 5")
        var rowCount = 0
        while (resultSet.next()) {
            rowCount += 1
        }
        assertEquals(1, rowCount, "Nested transaction did not rollback.")
    }

    @Test
    fun `Can nest transactions inside nested transactions`(){
        db.transaction() {
            try {
                val resultSet = query("select * from foo where num is null")
                resultSet.next()
                val id = resultSet.int("id")
                val num = 5
                resultSet.close()
                execute("update foo set num = :num where id = :id;") {
                    parameters.bind(hashMapOf("num" to num, "id" to id))
                }
                nest("a") {
                    execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                        parameters.bind(hashMapOf("bar" to "bare", "baz" to "bazf", "num" to 6))
                    }
                    nest("b") {
                        execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                            parameters.bind(hashMapOf("bar" to "barf", "baz" to "bazg", "num" to 7))
                        }
                        TransactionResult.ROLLBACK
                    }
                    return@nest TransactionResult.COMMIT
                }
            } catch (ex: Exception) {
                fail("Test failed with exception: $ex")
            }
            return@transaction TransactionResult.COMMIT
        }
        val resultSet = db.query("select * from foo where num >= 5")
        var rowCount = 0
        while (resultSet.next()) {
            rowCount += 1
        }
        assertEquals(2, rowCount, "Nested, nested transaction did not roll back.")
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
        db.close()
    }
}
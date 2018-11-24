package com.synappticlabs.db.sqlite3

import kotlin.test.*
import kotlin.native.concurrent.*

import platform.darwin.*
import platform.Foundation.*

class DatabaseWorkerManagerTests {
    val mgr = DatabaseWorkerManager.with("test.db")
    val data = listOf(
        hashMapOf("bar" to "bara", "baz" to "bazb", "num" to 1),
        hashMapOf("bar" to "barb", "baz" to "bazc", "num" to 2),
        hashMapOf("bar" to "barc", "baz" to "bazd", "num" to 3),
        hashMapOf("bar" to "bard", "baz" to "baze", "num" to null)
    )

    @Test
    fun `Can open and write to a database`() {
        var consumed = false
        mgr.readWriteAsync({"Async executed!"}) { context ->
            val db: Database = context.database
            db.execute("""
                drop table if exists foo;
            """.trimIndent())
            db.execute("""
                create table foo(
                    id integer primary key autoincrement,
                    bar text not null,
                    baz text not null,
                    num integer
                );
                """.trimIndent())

            return@readWriteAsync true
        }.consume { result ->
            assertTrue(result)
            consumed = true
        }
        assertTrue(consumed, "Future was not fully consumed before method ended")

        consumed = false
        mgr.readWriteAsync({"Async executed!"}) { context ->
            val db: Database = context.database
            return@readWriteAsync db.execute("insert into foo(bar, baz) values (\"a\",\"b\");")
        }.consume { result ->
            when (result) {
                is SQLiteResult.ModificationResult -> {
                    assertNotEquals(0, result.lastRowInsertId)
                }
                is SQLiteResult.FailureResult -> fail("Sqlite operation failed with $result.message")
            }
            consumed = true
        }
        assertTrue(consumed, "Second future was not fully consumed before method ended")
    }

    @Test
    fun `Passing in dependent data to a write is possible.`() {
        mgr.readWriteAsync({data}) { context ->
            val db: Database = context.database
            val data = context.data
            return@readWriteAsync db.transaction() {
                data.forEach { args ->
                    execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                        parameters.bind(args)
                    }
                }
                return@transaction TransactionResult.COMMIT
            }
        }.consume { result ->
            when (result) {
                is SQLiteResult.FailureResult -> {
                    fail("Failed to insert data.")
                }
                else -> {
                    assertTrue(result.success)
                }
            }
        }
    }

    @Test
    fun `Writing can happen from a background queue`() {
        val queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0)
        val serviceGroup = dispatch_group_create()
        for (index in 0..4) {
            dispatch_group_enter(serviceGroup)
            dispatch_async(queue, {
                mgr.readWriteAsync({data}) { context ->
                    val db: Database = context.database
                    val data = context.data
                    return@readWriteAsync db.transaction() {
                        data.forEach { args ->
                            execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                                parameters.bind(args)
                            }
                        }
                        return@transaction TransactionResult.COMMIT
                    }
                }.consume { result ->
                    dispatch_group_leave(serviceGroup)
                    assertTrue(result.success)
                }
            }.freeze())
        }
        dispatch_group_wait(serviceGroup,DISPATCH_TIME_FOREVER)
    }

    @Test
    fun `Can write from multiple operations`() {
        val queue = NSOperationQueue()
        queue.maxConcurrentOperationCount = 3 //Roughly 3 work operations will queue up at one time.
        for (index in 0..10) {
            queue.addOperationWithBlock({
                mgr.readWriteAsync({data}) { context ->
                    val db: Database = context.database
                    val data = context.data
                    return@readWriteAsync db.transaction() {
                        data.forEach { args ->
                            execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                                parameters.bind(args)
                            }
                        }
                        return@transaction TransactionResult.COMMIT
                    }
                }.consume { result ->
                    assertTrue(result.success)
                }
            }.freeze())
        }
        queue.waitUntilAllOperationsAreFinished()
    }

    @BeforeEach fun setup() {
        mgr.readWriteAsync({}) { context ->
            val db: Database = context.database
            db.execute("""
                drop table if exists foo;
                """)
            db.execute("""
                create table foo(
                    id integer primary key autoincrement,
                    bar text not null,
                    baz text not null,
                    num integer
                );
                """)
            return@readWriteAsync true
        }.consume { _ ->

        }
    }

    @AfterEach fun cleanup() {
    }
}
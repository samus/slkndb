package com.synappticlabs.db.sqlite3

import kotlin.test.*
import kotlin.native.concurrent.*

import platform.darwin.*
import platform.Foundation.*

class DatabaseWorkerManagerReadTests {
    val mgr = DatabaseWorkerManager.with("read.db")
    val data = listOf(
        hashMapOf("bar" to "bara", "baz" to "bazb", "num" to 1),
        hashMapOf("bar" to "barb", "baz" to "bazc", "num" to 2),
        hashMapOf("bar" to "barc", "baz" to "bazd", "num" to 3),
        hashMapOf("bar" to "bard", "baz" to "baze", "num" to null)
    )

    @Test
    fun `Reading can happen from background queues`() {
        val queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0)
        val serviceGroup = dispatch_group_create()
        for (index in 0..4) {
            dispatch_group_enter(serviceGroup)
            dispatch_async(queue, {
                mgr.readAsync({ index }) { context ->
                    val db: Database = context.database
                    val resultSet = db.query("select count(num) from foo where num = 1")
                    resultSet.next()
                    return@readAsync resultSet
                }.consume { resultSet ->
                    assertTrue(resultSet.columnCount > 0, "No columns returned from foo query.")
                    assertEquals(1, resultSet.int(0), "Count of num column was wrong.")
                    resultSet.close()
                    dispatch_group_leave(serviceGroup)
                }
            }.freeze())
        }
        dispatch_group_wait(serviceGroup,DISPATCH_TIME_FOREVER)
    }

    @Test
    fun `Can read from multiple operations`() {
        val queue = NSOperationQueue()
        queue.maxConcurrentOperationCount = 5 //Roughly 3 work operations will queue up at one time.
        for (index in 0..20) {
            queue.addOperationWithBlock({
                mgr.readAsync({ index }) { context ->
                    val db: Database = context.database
                    val resultSet = db.query("select count(num) from foo where num = 1")
                    assertTrue(resultSet.next())
                    return@readAsync resultSet
                }.consume { resultSet ->
                    assertTrue(resultSet.columnCount > 0, "No columns returned from foo query.")
                    assertEquals(1, resultSet.int(0), "Count of num column was wrong.")
                    resultSet.close()
                }
            }.freeze())
        }
        queue.waitUntilAllOperationsAreFinished()
    }

    @Test
    fun `Can use a value produced from a future`(){
        var actual = 0
        mgr.readAsync({}) { context ->
            return@readAsync context.database.query("select count(num) from foo where num = 1")
        }.consume { resultSet ->
            assertTrue(resultSet.next())
            resultSet.int(0)?.let { actual = it }
        }
        assertEquals(1, actual)
    }

    @BeforeEach
    fun setup() {
        mgr.readWriteAsync({}) { context ->
            val db: Database = context.database
            db.execute(
                """
                drop table if exists foo;
                """
            )
            db.execute(
                """
                create table foo(
                    id integer primary key autoincrement,
                    bar text not null,
                    baz text not null,
                    num integer
                );
                """
            )
            data.forEach { args ->
                db.execute("insert into foo(bar, baz, num) values (:bar,:baz,:num);") {
                    parameters.bind(args)
                }
            }
            return@readWriteAsync true
        }.consume { _ ->
        }
    }

    @AfterEach
    fun cleanup() {
    }
}
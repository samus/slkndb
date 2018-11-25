package com.synappticlabs.db.sqlite3

import kotlin.native.concurrent.*
import kotlinx.cinterop.*
import sqlite3.*

/**
 * Simple class that provides context to a worker job.
 * @param database the database connection to use in the job.
 * @param data Any data produced from a producer function.
 */
data class WorkerContext<T>(val database: Database, val data: T)

/**
 * DatabaseWorkerManager manages a pool of database objects and Kotlin Workers.
 * The number of writers is always one but the number of readers can be configured in the constructor.
 * All operations that need to write to the database should use the readWriteAsync method while reads
 * should use readAsync.  Note: this code is experimental and may not be an optimal solution.
 * It is likely to receive breaking changes in the near future.
 * @param path the location of the database file.  If the file doesn't exist it will be created.
 * @param readPoolSize the number of workers to use for reads.  The default size is two read workers.
 * @see https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.native.concurrent/-worker/index.html
 * @see https://github.com/JetBrains/kotlin-native/blob/master/CONCURRENCY.md
 */
class DatabaseWorkerManager(internal val path: String, val readPoolSize: Int = 2) {
    internal val readwriteWorker =  Worker.start()
    internal val readwriteDB = Database.open(path)
    internal val readWorkers = Array(readPoolSize, { Worker.start() })
    internal val readDB = Array(readPoolSize, { Database.open(path, readOnly = true) })
    internal val readIndex = AtomicInt(-1)

    /**
     * Sends a unit of work to the readWrite worker.  What is returned is a future that will block until
     * the worker has run and produced a result.  All functions passed into this method must
     * follow the rules of data passing between threads in Kotlin.
     * @see https://github.com/JetBrains/kotlin-native/blob/master/CONCURRENCY.md
     * @param producer a function that produces context data to be passed to the job.
     * @param job a function receiving a worker context.  Use the context.database object to
     * make calls that read and write to the database.  The value returned from this function is
     * the result produced from the Future.consume method.
     * @return Future<R> holding the data to be produced from the job function.  Call consume on it to
     * receive the value.
     */
    fun <T, R>readWriteAsync(producer: ()->T, job: (WorkerContext<T>) -> R): Future<R> {
        val pc = {
            val context = WorkerContext<T>(readwriteDB, producer())
            Pair(context, job)
        }.freeze()
        return readwriteWorker.execute(TransferMode.SAFE, pc) { it ->
            val work = it.second
            val context = it.first
            work(context)
        }
    }

    /**
     * Sends a unit of work one of the read only workers.  What is returned is a future that will block until
     * the worker has run and produced a result.  All functions passed into this method must
     * follow the rules of data passing between threads in Kotlin.  Currently a simple round robin algorithm is used
     * to determine which worker to send the operation to.  Due to the varying nature of the work sent to the workers
     * this could lead to some workers receiving additional work while others are idle.  Some benefit may be derived
     * by increasing the readPoolSize in read heavy applications.
     * @see https://github.com/JetBrains/kotlin-native/blob/master/CONCURRENCY.md
     * @param producer a function that produces context data to be passed to the job.
     * @param job a function receiving a worker context.  Use the context.database object to
     * make calls that read from the database.  The value returned from this function is
     * the result produced from the Future.consume method.
     * @return Future<R> holding the data to be produced from the job function.  Call consume on it to
     * receive the value.
     */
    fun <T, R>readAsync(producer: ()->T, job: (WorkerContext<T>) -> R): Future<R>  {
        val index = readIndex.addAndGet(1) % readPoolSize
        val db = readDB[index]
        val worker = readWorkers[index]
        val pc = {
            val context = WorkerContext<T>(db, producer())
            Pair(context, job)
        }.freeze()
        return worker.execute(TransferMode.SAFE, pc) { it ->
            val work = it.second
            val context = it.first
            work(context)
        }
    }

    /**
     * Closes all of the database objects.
     */
    fun close() {
        readwriteDB.close()
        readDB.forEach { it.close () }
    }

    companion object {
        fun with(path: String, readPoolSize: Int = 2): DatabaseWorkerManager {
            return DatabaseWorkerManager(path, readPoolSize)
        }
    }
}

    //inline fun <reified T, R> Worker.executeAsync(crossinline producerConsumer: () -> Pair<T, (T) -> R>): Future<R> =
     //   execute(TransferMode.SAFE, { producerConsumer() }) { it -> it.second(it.first) }
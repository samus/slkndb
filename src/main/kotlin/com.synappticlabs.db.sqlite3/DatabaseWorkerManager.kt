package com.synappticlabs.db.sqlite3

import kotlin.native.concurrent.*
import kotlinx.cinterop.*
import sqlite3.*

data class WorkerContext<T>(val database: Database, val data: T)

class DatabaseWorkerManager(internal val path: String) {
    internal val readwriteWorker =  Worker.start()
    internal var readwriteDB = Database.open(path)

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

    companion object {
        fun with(path: String): DatabaseWorkerManager {
            return DatabaseWorkerManager(path)
        }
    }
}

    //inline fun <reified T, R> Worker.executeAsync(crossinline producerConsumer: () -> Pair<T, (T) -> R>): Future<R> =
     //   execute(TransferMode.SAFE, { producerConsumer() }) { it -> it.second(it.first) }
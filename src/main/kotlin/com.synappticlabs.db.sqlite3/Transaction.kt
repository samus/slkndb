package com.synappticlabs.db.sqlite3

import kotlinx.cinterop.*
import sqlite3.*

class Transaction(private val db: Database) {

    fun execute(sql: String, function: (Statement.() -> Unit)? = null): SQLiteResult {
        return db.execute(sql, function)
    }

    fun query(sql: String, function: (Statement.() -> Unit)? = null): ResultSet {
        return db.query(sql, function)
    }

    fun nest(name: String, function: (Transaction.() -> TransactionResult)): SQLiteResult {
        val result = db.execute("savepoint ${name}")
        when (result) {
            is SQLiteResult.FailureResult -> return result
            else -> {
                when(function(this)) {
                    is TransactionResult.Commit -> return db.execute("release savepoint $name")
                    else -> return db.execute("rollback transaction to savepoint $name")
                }
            }
        }
    }

    companion object {
        internal fun begin(type: TransactionType, db: Database): Pair<SQLiteResult, Transaction?> {
            val result = db.execute("begin ${type.value} transaction")
            when (result) {
                is SQLiteResult.ModificationResult -> return Pair(result, Transaction(db))
                is SQLiteResult.FailureResult -> return Pair(result, null)
            }
        }

        internal fun end(type: TransactionResult, db: Database): SQLiteResult {
            return db.execute("${type.value} transaction")
        }
    }
}

/**
 * @see https://www.sqlite.org/lang_transaction.html
 */
sealed class TransactionType(val value: String) {
    class Deferred(): TransactionType("deferred")
    class Immediate(): TransactionType("immediate")
    class Exclusive(): TransactionType("exclusive")
}

sealed class TransactionResult(val value: String) {
    class Commit(): TransactionResult("commit")
    class Rollback(): TransactionResult("rollback")
}
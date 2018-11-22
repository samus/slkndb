package com.synappticlabs.db.sqlite3

import kotlinx.cinterop.*
import sqlite3.*

/**
 * Represents a transaction within the database.  Transactions allow writes to be batched together in one unit.
 * Depending on the success of the unit the entire set can either be committed or undone.
 */
class Transaction(private val db: Database) {
    /**
     * @see Database.execute
     */
    fun execute(sql: String, function: (Statement.() -> Unit)? = null): SQLiteResult {
        return db.execute(sql, function)
    }

    /**
     * @see Database.query
     */
    fun query(sql: String, function: (Statement.() -> Unit)? = null): ResultSet {
        return db.query(sql, function)
    }

    /**
     * Nest a transaction inside of the current transaction.  Sqlite doesn't actually have nested transactions.
     * Instead it uses an equivalent named savepoint mechanism with a syntax that differs from typical transaction
     * nesting syntax.  Internally, this method is issuing the savepoint commands to give the feel of a nested
     * transaction.  The savepoints are required to have a name so that sqlite can reference them.
     * @param The name of the inner transaction.
     * @param function A function to execute any statements and queries inside of.  The function should return
     * a value denoting whether the inner transaction should be committed or rolled back.
     * @return Whether the transaction was successfully commited or rolled back.
     */
    fun nest(name: String, function: (Transaction.() -> TransactionResult)): SQLiteResult {
        val result = db.execute("savepoint ${name}")
        when (result) {
            is SQLiteResult.FailureResult -> return result
            else -> {
                when(function(this)) {
                    TransactionResult.COMMIT -> return db.execute("release savepoint $name")
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
enum class TransactionType(val value: String) {
    DEFERRED("deferred"),
    IMMEDIATE("immediate"),
    EXCLUSIVE("exclusive")
}

/**
 * The completion action for a transaction.  This enum is used for both transactions and nested transactions.
 * Sqlite doesn't actually support nested transactions.  It uses a save point mechanism.  Internally when returning
 * a commit or rollback result from a nested transaction block, a release savepoint or rollback to savepoint
 * command will be issued.
 * @see https://www.sqlite.org/lang_transaction.html
 */
enum class TransactionResult(val value: String) {
    COMMIT("commit"),
    ROLLBACK("rollback")
}
package com.synappticlabs.db.sqlite3

import kotlinx.cinterop.*
import sqlite3.*


internal typealias DbConnection = CPointer<sqlite3>?

class Database(internal val conn: DbConnection) {
    /**
     * The opened state of the database connection.
     */
    var opened: Boolean = true
        private set

    /**
     * Attempts to verify the connection to the database is working properly.
     */
    fun goodConnection(): Boolean {
        if (opened == false) { return false }
        try {
            val rs = this.query("select name from sqlite_master where type='table'")
            val result = rs.next()
            rs.close()
            return result
        } catch (ex: Exception) {
            return false
        }
    }

    /**
     * Executes a sql statement in which no return value from the database is necessary except success/failure.
     * The sql will be compiled into a prepared statement according to
     * [`sqlite3_prepare_v2`](http://sqlite.org/c3ref/prepare.html)
     * and [`sqlite3_bind`](http://sqlite.org/c3ref/bind_blob.html).  The statement can contain use the unnamed
     * parameter binding `?` or use a named parameter in the format of `:alpha_numeric`.
     * Note: If modifications are being done on multiple threads result values could refer to the result from another thread.
     * see https://www.sqlite.org/capi3ref.html#sqlite3_last_insert_rowid
     * @param sql The sql to be executed including optional placeholders.
     * @param function a callback allowing for flexible configuration of the statement.  The function's `this` context
     * is the statement.  Any parameters that need to be bound can call on the `parameters` object.
     * @return SQLiteResult with either a success or a failure result.
     * @throws SQLiteError if an error is encountered while executing the statement.
     */
    @Throws()
    fun execute(sql: String, function: (Statement.() -> Unit)? = null): SQLiteResult {
        val stmt = Statement.prepare(sql, conn)
        try {
            function?.let { it(stmt) }
            return if (stmt.execute()) {
                SQLiteResult.success(this)
            } else SQLiteResult.failure(this)
        } finally {
            stmt.close()
        }
    }

    /**
     * Executes a sql statement that returns results and wraps it in a ResultSet. The sql will be compiled into
     * a prepared statement according to [`sqlite3_prepare_v2`](http://sqlite.org/c3ref/prepare.html)
     * and [`sqlite3_bind`](http://sqlite.org/c3ref/bind_blob.html).  The statement can contain use the unnamed
     * parameter binding `?` or use a named parameter in the format of `:alpha_numeric`.
     *
     * When reading data from the ResultSet it is necessary to first call `next()` otherwise no data will be
     * available and `hasNext` will not return an accurate result.
     * @param sql The sql to be executed including optional placeholders.
     * @param function a callback allowing for flexible configuration of the statement.  The function's `this` context
     * is the statement.  Any parameters that need to be bound can call on the `parameters` object.
     * @return A ResultSet which can be used to read rows and columns of data.
     */
    fun query(sql: String, function: (Statement.() -> Unit)? = null): ResultSet {
        val stmt = Statement.prepare(sql, conn)
        function?.let { it(stmt) }
        return ResultSet(stmt)
    }

    /**
     * Create a unit of work in the database that can be committed or rolled back as a single piece.
     * @param type the type of the transaction to start.  See the sqlite docs for more information.
     * @param function Code to execute within the transaction.  The function must return a result signifying whether
     * the work is to be committed or rolled back.
     * @see https://www.sqlite.org/lang_transaction.html
     */
    fun transaction(type: TransactionType = TransactionType.DEFERRED,
                    function: (Transaction.() -> TransactionResult)): SQLiteResult {
        val (begin, transaction) = Transaction.begin(type, this)

        when (begin) {
            is SQLiteResult.FailureResult -> return begin
            else -> {
                if (transaction == null) {
                    return SQLiteResult.FailureResult(-1, "Unknown problem starting transaction")
                }
                try {
                    return Transaction.end(function(transaction), this)
                } catch (ex: Exception) {
                    return Transaction.end(TransactionResult.ROLLBACK, this)
                }
            }
        }
    }

    /**
     * Close the connection to a database file.  If there are leaked or open statements
     * an attempt will be made to cleanly finalize them.
     * @return True if the database connection was cleanly closed.
     * @throws SQLiteError if the database connection could not be cleanly shutdown.
     */
    @Throws()
    fun close(): Boolean {
        if (opened == false) {
            return true
        }
        var rc: Int
        var retry: Boolean
        var tryFinalizingStatements = true

        do {
            retry = false
            rc = sqlite3_close(conn)
            when (rc) {
                SQLITE_BUSY, SQLITE_LOCKED -> {
                    if (tryFinalizingStatements) {
                        tryFinalizingStatements = false
                        var statement = sqlite3_next_stmt(conn, null)
                        while (statement != null) {
                            print("Closing leaked statement")
                            sqlite3_finalize(statement)
                            retry = true
                            statement = sqlite3_next_stmt(conn, null)
                        }
                    }
                }
                SQLITE_OK -> {
                }
                else -> {
                    throw SQLiteError("Cannot close database code: $rc")
                }
            }
        } while (retry)

        this.opened = false
        return true
    }

    internal fun lastInsertRowId(): Long = sqlite3_last_insert_rowid(conn)

    internal fun resetLastRowId(id: Long = 0) = sqlite3_set_last_insert_rowid(conn, id)

    internal fun changeCount(): Int = sqlite3_changes(conn)

    internal fun lastErrorMessage(): String? = sqlite3_errmsg(conn)?.toKString()

    internal fun lastErrorCode(): Int = sqlite3_errcode(conn)

    companion object {
        val sqliteVersion: String
            get() {
                return sqlite3_libversion()?.toKString() ?: ""
            }

        val sqliteVersionNumber: Int
            get() {
                return sqlite3_libversion_number()
            }

        /**
         * Create a Database object using a path to a sqlite file.  The database file will be opened or
         * created at the specified path if it does not exist.
         * @param path The path to the database file or :memory: for an in memory database
         * @param readOnly By default the database is opened in readwrite mode.
         * @return A Database with an open connection to the file.
         * @throws SQLiteError if the database cannot be opened.
         */
        @Throws()
        fun open(path: String = ":memory:", readOnly: Boolean = false): Database {
            return memScoped {
                val dbPtr = alloc<CPointerVar<sqlite3>>()
                var flags = SQLITE_OPEN_CREATE
                if (readOnly) {
                    flags = SQLITE_OPEN_READONLY
                } else {
                    flags = flags or SQLITE_OPEN_READWRITE
                }
                if (sqlite3_open_v2(path, dbPtr.ptr, flags, null) != 0) {
                    throw SQLiteError("Cannot open database: ${sqlite3_errmsg(dbPtr.value)?.toKString()} at path: $path")
                }
                Database(dbPtr.value!!)
            }
        }
    }
}

sealed class SQLiteResult(val success: Boolean) {
    data class ModificationResult(val lastRowInsertId: Long, val modificationCount: Int): SQLiteResult(true)
    data class FailureResult(val code: Int, val message: String): SQLiteResult(false)

    companion object {
        internal fun success(db: Database, resetLastRow: Boolean = true): ModificationResult {
            val rowId = db.lastInsertRowId()
            val count = db.changeCount()
            if (resetLastRow) { db.resetLastRowId() }

            return ModificationResult(rowId, count)
        }
        internal fun failure(db: Database): FailureResult {
            val code = db.lastErrorCode()
            val message = db.lastErrorMessage() ?: "Unknown"
            return FailureResult(code, message)
        }
    }
}
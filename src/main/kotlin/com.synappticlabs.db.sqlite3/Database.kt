package com.synappticlabs.db.sqlite3

import kotlinx.cinterop.*
import sqlite3.*


typealias DbConnection = CPointer<sqlite3>?

class Database(internal val conn: DbConnection) {
    /**
     * The opened state of the database connection.
     */
    var opened: Boolean = true
        private set

    /**
     * Close the connection to a database file.  If there are leaked or open statements
     * an attempt will be made to cleanly finalize them.
     * @return True if the database connection was cleanly closed.
     * @throws SQLiteError if the database connection could not be cleanly shutdown.
     */
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

    /**
     * Attempts to verify the connection to the database is working properly.
     */
    fun goodConnection(): Boolean {
        if (opened == false) { return false }

        TODO("Execute a query to determine if the connection is good.")

        return true
    }

    /**
     * Executes a sql statement
     */
    fun execute(sql: String, function: ((Statement) -> Unit)? = null): Boolean {
        val stmt = Statement.prepare(sql, conn)
        try {
            function?.let { it(stmt) }
            return stmt.step()
        } finally {
            stmt.close()
        }

    }

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
        fun open(path: String = ":memory:", readOnly: Boolean = false): Database {
            return memScoped {
                val dbPtr = alloc<CPointerVar<sqlite3>>()
                var flags = SQLITE_OPEN_CREATE
                if (readOnly) {
                    flags = flags or SQLITE_OPEN_READONLY
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
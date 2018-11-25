package com.synappticlabs.db.sqlite3

import kotlinx.cinterop.*
import sqlite3.*

internal typealias StatementPointer = CPointer<sqlite3_stmt>

class Statement(internal val stmt: StatementPointer, private val conn: DbConnection) {
    val parameters = ParameterBinder(stmt)

    /**
     * Executes the statement.
     * @return true if the statement fully completed.
     * @throws SQLiteError if sqlite encountered an error.
     */
    internal fun execute(): Boolean {
        return when(step()) {
            SQLITE_ROW -> false
            SQLITE_DONE -> true
            else -> throw SQLiteError("Error executing statement", conn)
        }
    }

    /**
     * The raw step function.
     * @return the SQLITE_*** result code.
     * @see https://www.sqlite.org/capi3ref.html#sqlite3_step
     */
    internal fun step(): Int = sqlite3_step(stmt)

    /**
     * Reset sets a statment back to its initial state of execution.  It does not clear any bound parameters.
     * @throws SQLiteError if the statement could not be reset.
     */
    fun reset() = sqliteTryOrThrow(conn, "Unable to reset statement") { sqlite3_reset(stmt) }

    /**
     * Clears the values from the bound parameters.
     * @throws SQLiteError if the clear function fails.
     */
    fun clear() = sqliteTryOrThrow(conn, "Unable to clear bindings") { sqlite3_clear_bindings(stmt) }

    private var closed = false
    /**
     * All statements must be closed or else they will leak resources.
     * @throws SQLiteError if the statement was unable to be closed.
     */
    fun close() {
        if (closed) { return }
        closed = true
        sqliteTryOrThrow(conn, "Unable to close statement"){ sqlite3_finalize(stmt) }
    }

    companion object {
        @Throws()
        internal fun prepare(sql: String, conn: DbConnection): Statement = memScoped {
            val stmt = alloc<CPointerVar<sqlite3_stmt>>()

            if (sqlite3_prepare_v3(conn, sql.cstr.ptr, sql.length, 0, stmt.ptr, null) != SQLITE_OK) {
                throw SQLiteError("Unable to prepare statement", conn)
            }
            Statement(stmt.value!!, conn)
        }
    }
}
package com.synappticlabs.db.sqlite3

import kotlinx.cinterop.*
import sqlite3.*

typealias StatementPointer = CPointer<sqlite3_stmt>

class Statement(private val stmt: StatementPointer, private val conn: DbConnection) {

    fun step(): Boolean {
        return when(sqlite3_step(stmt)) {
            SQLITE_ROW -> false
            SQLITE_DONE -> true
            else -> throw SQLiteError("Error stepping", conn)
        }
    }

    fun close() {
        sqliteTryOrThrow(conn, "Unable to close statement"){ sqlite3_finalize(stmt) }
    }

    companion object {
        internal fun prepare(sql: String, conn: DbConnection): Statement = memScoped {
            val stmt = alloc<CPointerVar<sqlite3_stmt>>()

            if (sqlite3_prepare_v3(conn, sql.cstr.ptr, sql.length, 0, stmt.ptr, null) != SQLITE_OK) {
                throw SQLiteError("Unable to prepare statement", conn)
            }
            Statement(stmt.value!!, conn)
        }
    }
}
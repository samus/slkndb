package com.synappticlabs.db.sqlite3

import kotlinx.cinterop.*
import sqlite3.*

class SQLiteError(message: String) : Error(message) {
    constructor(message: String, conn: DbConnection) : this("$message: ${sqlite3_errmsg(conn)?.toKString()}")
}

inline fun sqliteTryOrThrow(conn: DbConnection, message: String, function: ()-> Int) {
    if (function() != SQLITE_OK) {
        throw SQLiteError(message, conn)
    }
}
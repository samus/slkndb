package com.synappticlabs.db.sqlite3

import kotlinx.cinterop.*
import sqlite3.*

class ResultSet(private val statement: Statement) {
    internal val nameIndexMap: Map<String, Int> by lazy {
        val map = HashMap<String, Int>()
        for (index in 0 until columnCount) {
            val name = nameOf(index)
            map[name] = index
        }
        return@lazy map
    }

    var hasNext: Boolean = false
        private set

    @Throws()
    fun next(): Boolean {
        when (statement.step()) {
            SQLITE_ROW -> {
                hasNext = true
                return true
            }
            SQLITE_DONE -> {
                this.close()
                return false
            }
            SQLITE_BUSY -> throw SQLiteError("Database busy")
            SQLITE_LOCKED -> throw SQLiteError("Database locked")
            else -> throw SQLiteError("Unknown error occurred while stepping statement.")
        }
    }

    val columnCount: Int get() = sqlite3_column_count(statement.stmt)

    fun indexOf(name: String): Int? {
        return nameIndexMap[name]
    }

    @Throws()
    fun nameOf(index: Int): String {
        checkColumnIndex(index)
        return sqlite3_column_name(statement.stmt, index)!!.toKString()
    }

    @Throws()
    fun string(name: String): String? {
        val index = indexOfOrThrow(name)
        return string(index)
    }

    @Throws()
    fun string(index: Int): String? {
        checkColumnIndex(index)
        if (checkColumnNull(index)) { return null }
        return sqlite3_column_text(statement.stmt, index)?.reinterpret<ByteVar>()?.toKString()
    }

    @Throws()
    fun int(name: String): Int? {
        val index = indexOfOrThrow(name)
        return int(index)
    }

    @Throws()
    fun int(index: Int): Int? {
        checkColumnIndex(index)
        if (checkColumnNull(index)) { return null }
        return sqlite3_column_int(statement.stmt, index)
    }

    @Throws()
    fun long(name: String): Long? {
        val index = indexOfOrThrow(name)
        return long(index)
    }

    @Throws()
    fun long(index: Int): Long? {
        checkColumnIndex(index)
        if (checkColumnNull(index)) { return null }
        return sqlite3_column_int64(statement.stmt, index)
    }

    @Throws()
    fun double(name: String): Double? {
        val index = indexOfOrThrow(name)
        return double(index)
    }

    @Throws()
    fun double(index: Int): Double? {
        checkColumnIndex(index)
        if (checkColumnNull(index)) { return null }
        return sqlite3_column_double(statement.stmt, index)
    }

    @Throws()
    fun byteArray(name: String): ByteArray? {
        val index = indexOfOrThrow(name)
        return byteArray(index)
    }

    @Throws()
    fun byteArray(index: Int): ByteArray? {
        checkColumnIndex(index)
        if (checkColumnNull(index)) { return null }
        val ptr = sqlite3_column_blob(statement.stmt, index)
        val size = sqlite3_column_bytes(statement.stmt, index)

        if (ptr == null) { return null }
        if (size == 0) { return byteArrayOf() }

        return ptr.readBytes(size)
    }

    @Throws()
    fun close() {
        hasNext = false
        statement.close()
    }

    private fun checkColumnIndex(index: Int) {
        if (index in 0 until columnCount) { return }
        throw IllegalArgumentException ("Colum index $index is out of bounds for column count ${columnCount}." )
    }

    private fun indexOfOrThrow(name: String): Int {
        return indexOf(name) ?: throw IllegalArgumentException("Column name not found.")
    }

    private fun checkColumnNull(index: Int): Boolean {
        return sqlite3_column_type(statement.stmt, index) == SQLITE_NULL
    }
}
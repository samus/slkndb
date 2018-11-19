package com.synappticlabs.db.sqlite3

import kotlinx.cinterop.*
import sqlite3.*

class ResultSet(private val statement: Statement) {
    internal val nameIndexMap: Map<String, Int> by lazy {
        val map = HashMap<String, Int>()
        for (index in 0..columnCount) {
            val name = nameOf(index)
            map[name] = index
        }
        return@lazy map
    }


    fun close() {
        statement.close()
    }

    fun next() {
        TODO("Implement next")
    }

    val hasNext: Boolean
        get() { TODO("Implement next") }

    val columnCount: Int get() = sqlite3_column_count(statement.stmt)

    fun indexOf(name: String): Int? {
        return nameIndexMap[name]
    }

    fun nameOf(index: Int): String {
        checkColumnIndex(index)
        return sqlite3_column_name(statement.stmt, index)!!.toKString()
    }

    fun <T>value(index: Int): T {
        TODO("Implement reading values")
    }

    internal fun checkColumnIndex(index: Int) {
        check(index in 0 until columnCount) { "Colum index $index is out of bounds for column count ${columnCount}." }
    }
}
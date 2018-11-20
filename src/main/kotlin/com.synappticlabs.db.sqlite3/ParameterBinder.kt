package com.synappticlabs.db.sqlite3

import kotlinx.cinterop.*
import sqlite3.*

class ParameterBinder(internal val stmt: StatementPointer) {
    val count: Int get() = sqlite3_bind_parameter_count(stmt)

    /**
     * Binds a collection of values to the parameters.
     * @throws SQLiteError if there is a problem binding the parameter.
     */
    @Throws()
    fun bind(values: Iterable<Any?>) {
        values.withIndex().forEach() { entry ->
            this.bind(entry.index + 1, entry.value)
        }
    }

    /**
     * Binds a map of values to a sql statement.  The keys need to match the named parameters in the statment without
     * a semi-colon at the start of the key name.
     * @throws IllegalArgumentException if the named parameter is not found
     * @throws SQLiteError if there is a problem binding the parameter.
     */
    @Throws()
    fun bind(values: Map<String, Any?>) {
        values.forEach { (key, value) ->
            this.bind(name = key, value = value)
        }
    }

    /**
     * Binds a value to a named parameter.  Note: it is not necessary to specify a colon at the beginning of the name.
     * One will be inserted automatically.
     * @param name the name of the parameter as specified in a sql statement with a colon such as :col1.
     * @param value T the value of the parameter must be of type supported by `fun <T>bind(index: Int, value: T)`
     * @throws IllegalArgumentException if the named parameter is not found
     * @throws SQLiteError if there is a problem binding the parameter.
     */
    @Throws()
    fun <T>bind(name: String, value: T) {
        val index = parameterIndex(":$name")
        when (index) {
            0 -> throw IllegalArgumentException("Parameter name $name not found")
            else -> bind(index, value)
        }
    }

    /**
     * Binds a value indexed parameter.  The value must be one of String, ByteArray, Double, Int, Long or null.
     * Currently the [sqlite3_value](https://www.sqlite.org/capi3ref.html#sqlite3_value) type is not supported.
     * @param index 1 based index of the parameters in the statement.
     * @param value the value to bind.  See description for the supported types.
     * @throws SQLiteError if there is a problem binding the parameter.
     */
    @Throws()
    fun <T>bind(index: Int, value: T) {
        val rc = when (value) {
            is String -> sqlite3_bind_text(stmt, index, value, value.toUtf8().count(), (-1).toLong().toCPointer())
            is ByteArray -> sqlite3_bind_blob(stmt, index, value.refTo(0), value.size, (-1).toLong().toCPointer())
            is Double -> sqlite3_bind_double(stmt, index, value)
            is Int -> sqlite3_bind_int(stmt, index, value)
            is Long -> sqlite3_bind_int64(stmt, index, value)
            null -> sqlite3_bind_null(stmt, index)
            else -> throw SQLiteError("Unable to handle datatype.")
        }
        successOrThrow(rc)
    }

    private fun successOrThrow(rc: Int) {
        when (rc) {
            SQLITE_OK -> { return }
            SQLITE_TOOBIG -> throw SQLiteError("Value is too large")
            SQLITE_RANGE -> throw SQLiteError("Parameter index out of range")
            SQLITE_NOMEM -> throw SQLiteError("Malloc failed")
            else -> throw SQLiteError("Binding failed with unknown error")
        }
    }

    private fun parameterIndex(name: String): Int = sqlite3_bind_parameter_index(stmt, name)

}
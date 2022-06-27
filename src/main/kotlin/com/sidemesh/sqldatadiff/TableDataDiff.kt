package com.sidemesh.sqldatadiff

import java.sql.Connection
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.util.stream.Collectors

class TableDataDiff(
    private val connectionBase: Connection,
    private val connectionCompare: Connection,
    private val table: String,
    private var keyColumn: String,
    private val ignoreColumns: List<String> = listOf("id", "create_time", "update_time", "create_by", "update_by")
) {
    fun diff() {
        val baseResult = resultSetToArrayList(connectionBase)
        val toCompareResult = resultSetToArrayList(connectionCompare)

        val server2KeyMap = toCompareResult.stream().collect(Collectors.toMap({ it[keyColumn] }, { it }))

        println("-- Table: $table generated start")
        for (row in baseResult) {
            val server2Row = server2KeyMap.remove(keyColumn)
            if (server2Row != null && !rowEquals(row, server2Row)) {
                generateUpdateSql(row, server2Row)
            }
            if (server2Row == null) {
                generateDeleteSql(row)
            }
        }
        if (server2KeyMap.isNotEmpty()) {
            for (row in server2KeyMap.values) {
                generateInsertSql(row)
            }
        }

        println("-- Table: $table generated end")
    }

    private fun generateInsertSql(row: HashMap<String, Any?>) {
        val columns = ArrayList<String>()
        val values = ArrayList<String>()
        row.forEach { (t, u) ->
            run {
                columns.add("`$t`")
                values.add(toSql(u))
            }
        }
        println(
            "INSERT IGNORE `$table`(${columns.stream().collect(Collectors.joining(","))}) VALUES(${
                values.stream().collect(Collectors.joining(","))
            });"
        )
    }

    private fun generateUpdateSql(row: HashMap<String, Any?>, server2Row: HashMap<String, Any?>) {
        val setColumn = ArrayList<String>()
        row.forEach { (t, u) ->
            run {
                if (!ignoreColumns.contains(t) && !columnValueEqual(u, server2Row[t])) {
                    setColumn.add("`$t`=${toSql(server2Row[t])}")
                }
            }
        }
        setColumn.stream()
            .collect(Collectors.joining(","))
        println(
            "UPDATE `$table` SET ${
                setColumn.stream().collect(Collectors.joining(","))
            } WHERE `id` = ${row["id"]} AND `$keyColumn` = ${toSql(row[keyColumn])};"
        )
    }

    private fun generateDeleteSql(row: java.util.HashMap<String, Any?>) {
        println("DELETE FROM `$table` WHERE `id` = ${row["id"]} AND `$keyColumn` = ${toSql(row[keyColumn])};")
    }

    private fun rowEquals(row: HashMap<String, Any?>, server2Row: HashMap<String, Any?>): Boolean {
        for (column in row.keys) {
            if (!ignoreColumns.contains(column)) {
                if (row[column] != server2Row[column]) {
                    return false
                }
            }
        }
        return true
    }

    private fun columnValueEqual(data1: Any?, data2: Any?): Boolean {
        if (data1 == null && data2 == null) {
            return true
        }
        if (data1 == null || data2 == null) {
            return false
        }
        return data1.toString().trim() == data2.toString().trim()
    }

    @Throws(SQLException::class)
    private fun resultSetToArrayList(connection: Connection): ArrayList<HashMap<String, Any?>> {
        val stmt = connection.createStatement()
        val rs = stmt.executeQuery("select * from $table")
        val md: ResultSetMetaData = rs.metaData
        val columns: Int = md.columnCount
        val list = ArrayList<HashMap<String, Any?>>(50)
        while (rs.next()) {
            val row = HashMap<String, Any?>(columns)
            for (i in 1..columns) {
                row[md.getColumnName(i)] = rs.getObject(i)
            }
            list.add(row)
        }
        return list
    }

    private fun toSql(data: Any?): String {
        if (data == null) {
            return "NULL"
        }
        if (data is Number) {
            return data.toString()
        }
        return "'$data'".trim()
    }

}
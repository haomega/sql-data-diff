package com.sidemesh.sqldatadiff

import java.sql.Connection
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.util.Arrays
import java.util.LinkedList
import java.util.stream.Collectors

class TableDataDiff(
    private val connectionBase: Connection,
    private val connectionCompare: Connection,
    private val table: String,
    private val rowUniqueKey: RowUniqueKey,
    private val ignoreColumns: List<String> = listOf("id", "create_time", "update_time", "create_by", "update_by")
) {
    fun diff() {
        val baseResult = resultSetToArrayList(connectionBase)
        val toCompareResult = resultSetToArrayList(connectionCompare)

        val server2KeyMap: MutableMap<String, TableIdRow> =
            toCompareResult.stream().collect(Collectors.toMap({ rowUniqueKey.uniqueKey(it) }, { it }))

        println("-- Table: $table generated start")
        for (row in baseResult) {
            val key = rowUniqueKey.uniqueKey(row)
            val server2Row = server2KeyMap.remove(key)
            if (server2Row != null && !row.equals(server2Row)) {
                row.generateUpdateSql(server2Row)
            }
            if (server2Row == null) {
                row.generateDeleteSql()
            }
        }
        if (server2KeyMap.isNotEmpty()) {
            for (row in server2KeyMap.values) {
                row.generateInsertSql()
            }
        }

        println("-- Table: $table generated end")
    }


    @Throws(SQLException::class)
    private fun resultSetToArrayList(connection: Connection): ArrayList<TableIdRow> {
        val stmt = connection.createStatement()
        val rs = stmt.executeQuery("select * from $table")
        val md: ResultSetMetaData = rs.metaData
        val columns: Int = md.columnCount
        val list = ArrayList<TableIdRow>(50)
        while (rs.next()) {
            val row = TableIdRow(rs.getObject("id"))
            for (i in 1..columns) {
                row.addColumnValue(md.getColumnName(i), rs.getObject(i))
            }
            list.add(row)
        }
        return list
    }

    /**
     * TableIdRow is use `id` as primary key table
     */
    inner class TableIdRow(
        private val id: Any,
        private val data: HashMap<String, Any?> = HashMap(20)
    ) {

        fun addColumnValue(column: String, value: Any?) {
            data[column] = value
        }

        fun get(column: String): Any? {
            return data[column]
        }

        fun equals(otherRow: TableIdRow?): Boolean {
            if (otherRow == null) {
                return false
            }
            for (column in data.keys) {
                if (!ignoreColumns.contains(column)) {
                    if (data[column] != otherRow.get(column)) {
                        return false
                    }
                }
            }
            return true
        }

        fun generateInsertSql() {
            val columns = LinkedList<String>()
            val values = LinkedList<String>()
            data.forEach { (t, u) ->
                if ("id" != t) {
                    columns.add("`$t`")
                    values.add(toSqlSymbol(u))
                }
            }
            // id first
            columns.push("id")
            values.push(toSqlSymbol(id))
            println(
                "INSERT IGNORE `$table`(${columns.stream().collect(Collectors.joining(","))}) VALUES(${
                    values.stream().collect(Collectors.joining(","))
                });"
            )
        }

        fun generateUpdateSql(toUpdateRow: TableIdRow) {
            val setColumn = ArrayList<String>()
            data.forEach { (t, u) ->
                if (!ignoreColumns.contains(t) && !isColumnEqual(u, toUpdateRow.get(t))) {
                    setColumn.add("`$t`=${toSqlSymbol(toUpdateRow.get(t))}")
                }
            }
            setColumn.stream()
                .collect(Collectors.joining(","))
            println(
                "UPDATE `$table` SET ${setColumn.stream().collect(Collectors.joining(","))} " +
                        "WHERE `id` = $id AND ${rowUniqueKey.andSql(this)};"
            )
        }

        fun generateDeleteSql() {
            println("DELETE FROM `$table` WHERE `id` = $id AND ${rowUniqueKey.andSql(this)};")
        }

        private fun isColumnEqual(data1: Any?, data2: Any?): Boolean {
            if (data1 == null && data2 == null) {
                return true
            }
            if (data1 == null || data2 == null) {
                return false
            }
            return data1.toString().trim() == data2.toString().trim()
        }

    }

    class RowUniqueKey(private val columns: Array<String>) {
        fun uniqueKey(row: TableIdRow): String {
            return Arrays.stream(columns).map { row.get(it).toString() }.collect(Collectors.joining("_"))
        }

        fun andSql(row: TableIdRow): String {
            return Arrays.stream(columns).map { "`$it` = ${toSqlSymbol(row.get(it))}" }.collect(Collectors.joining(" AND "))
        }
    }

}
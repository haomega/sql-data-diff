package com.sidemesh.sqldatadiff


fun toSqlSymbol(data: Any?): String {
    if (data == null) {
        return "NULL"
    }
    if (data is Number) {
        return data.toString()
    }
    return "'$data'".trim()
}
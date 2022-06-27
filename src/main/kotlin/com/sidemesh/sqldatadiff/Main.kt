package com.sidemesh.sqldatadiff

import java.sql.DriverManager


fun main(args: Array<String>) {
    Class.forName("com.mysql.jdbc.Driver");
    // server1
    val s0614 = DriverManager.getConnection("jdbc:mysql://localhost:3308/phoenix_basic?useSSL=false", "root", "root123");
    // server2
    val s0625 = DriverManager.getConnection("jdbc:mysql://localhost:3307/phoenix_basic?useSSL=false", "root", "root123");

    TableDataDiff(s0614, s0625, "basic_dictionary", TableDataDiff.RowUniqueKey(arrayOf("group_code", "item_code")))
        .diff()
}



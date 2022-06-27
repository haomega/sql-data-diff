package com.sidemesh.sqldatadiff

import java.sql.DriverManager


fun main(args: Array<String>) {
    Class.forName("com.mysql.jdbc.Driver");
    // server1
    val s0614 = DriverManager.getConnection("jdbc:mysql://localhost:3308/phoenix_basic?useSSL=false", "root", "root123");
    // server2
    val s0625 = DriverManager.getConnection("jdbc:mysql://localhost:3307/phoenix_basic?useSSL=false", "root", "root123");
//
//    TableDataDiff("basic_error_info", "error_code", listOf("id", "create_time", "update_time", "create_by", "update_by"), server1, server2)
//        .diff()
//    TableDataDiff("basic_system_config", "name", listOf("id", "create_time", "update_time", "create_by", "update_by"), server1, server2)
//        .diff()
//    println("done")

    // system config 参照236.69
//    val server3 = DriverManager.getConnection("jdbc:mysql://172.31.236.69:3306/phoenix_basic?useSSL=false", "root", "root123");
    TableDataDiff(s0614, s0625, "basic_dictionary", "aaa")
        .diff()
}



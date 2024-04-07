package com.until

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

object Connection {

    //定义连接数据库方法
    def createConnection(): Connection = {
        val url = "jdbc:mysql://192.168.92.101:3306/taobao"
        //        val url = "jdbc:mysql://localhost:3306/taobao"
        val user = "root"
        val password = "123456"
        Class.forName("com.mysql.cj.jdbc.Driver")
        DriverManager.getConnection(url, user, password)
    }

    //定义存储数据到数据库方法
    def saveToDB(conn: Connection, record: (String, String, String, String, Timestamp)): Unit = {
        val sql = "insert into user_behavior(user_id, item_id, category_id, action, timestamp) values (?, ?, ?, ?, ?)"
        val statement: PreparedStatement = conn.prepareStatement(sql)
        statement.setString(1, record._1)
        statement.setString(2, record._2)
        statement.setString(3, record._3)
        statement.setString(4, record._4)
        statement.setTimestamp(5, record._5)
        statement.executeUpdate()
    }

}

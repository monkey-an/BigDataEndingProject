package com.aura.eight.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveConnectionManager {

    private static Connection conn = null;

    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static final String jdbcUrl = "jdbc:hive2://localhost:10000/jd";
    private static final String user = "bigdata";
    private static final String password = "bigdata";

    public synchronized static Connection getConn() {
        if (conn == null) {
            try {
                Class.forName(driverName);
                conn = DriverManager.getConnection(jdbcUrl, user, password);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    public static Connection getNewConn()
    {
        try {
            Class.forName(driverName);
             return DriverManager.getConnection(jdbcUrl, user, password);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public synchronized static void recoverConn()
    {
        try {
            if(conn.isClosed())
            {
                conn = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

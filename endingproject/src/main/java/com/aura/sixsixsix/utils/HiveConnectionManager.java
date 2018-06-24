package com.aura.sixsixsix.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveConnectionManager {

    private static Connection conn = null;

    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static final String jdbcUrl = "jdbc:hive2://anlu.local:10000/exam";
    private static final String user = "anluhive";
    private static final String password = "anluhive";

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

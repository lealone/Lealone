package com.codefollower.lealone.test.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JDBCExample {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:lealone:tcp://localhost:9092/hbasedb";
        Connection conn = DriverManager.getConnection(url, "sa", "");
        Statement stmt = conn.createStatement();
        
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");

        stmt.executeUpdate("INSERT INTO test(f1, f2) VALUES(1, 2)");
        stmt.executeUpdate("UPDATE test SET f2 = 1");
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        while (rs.next()) {
            System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
            System.out.println();
        }
        stmt.executeUpdate("DELETE FROM test WHERE f1 = 1");

        stmt.executeUpdate("DROP TABLE IF EXISTS test");
    }
}

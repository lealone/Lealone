/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.misc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Assert;

import com.lealone.db.LealoneDatabase;
import com.lealone.net.bio.BioNetFactory;
import com.lealone.test.TestBase;

public class CRUDExample {

    public static void main(String[] args) throws Exception {
        TestBase test = new TestBase();
        test.setNetFactoryName(BioNetFactory.NAME);
        Connection conn = test.getConnection(LealoneDatabase.NAME);
        crud(conn);
    }

    public static void crud(Connection conn) throws Exception {
        crud(conn, null);
    }

    public static void crud(Connection conn, String storageEngineName) throws Exception {
        Statement stmt = conn.createStatement();
        crud(stmt, storageEngineName);
        // batchInsert(stmt);
        // batchDelete(stmt);
        stmt.close();
        conn.close();
    }

    public static void crud(Statement stmt, String storageEngineName) throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        String sql = "CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)";
        if (storageEngineName != null)
            sql += " ENGINE = " + storageEngineName;
        stmt.executeUpdate(sql);

        stmt.execute("INSERT INTO test(f1, f2) VALUES(1, 1)");
        stmt.executeUpdate("UPDATE test SET f2 = 2 WHERE f1 = 1");
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        // ((JdbcStatement) stmt).executeQueryAsync("SELECT2 * FROM test").onComplete(ar -> {
        // ar.getCause().printStackTrace();
        // });
        Assert.assertTrue(rs.next());
        System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
        Assert.assertFalse(rs.next());
        rs.close();
        stmt.executeUpdate("DELETE FROM test WHERE f1 = 1");
        rs = stmt.executeQuery("SELECT * FROM test");
        Assert.assertFalse(rs.next());
        rs.close();
    }

    public static void batchInsert(Statement stmt) throws Exception {
        for (int i = 1; i <= 6000; i++)
            stmt.executeUpdate("INSERT INTO test(f1, f2) VALUES(" + i + ", " + i * 10 + ")");
        stmt.setFetchSize(10);
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        while (rs.next())
            rs.getInt(1);
    }

    public static void batchDelete(Statement stmt) throws Exception {
        for (int i = 1; i <= 60000; i++)
            stmt.executeUpdate("DELETE FROM test WHERE f1 =" + i);
    }
}

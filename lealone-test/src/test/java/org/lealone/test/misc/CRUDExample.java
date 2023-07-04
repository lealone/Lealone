/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Assert;
import org.lealone.db.LealoneDatabase;
import org.lealone.net.bio.BioNetFactory;
import org.lealone.test.TestBase;

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
        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        String sql = "CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)";
        if (storageEngineName != null)
            sql += " ENGINE = " + storageEngineName;
        stmt.executeUpdate(sql);

        stmt.executeUpdate("INSERT INTO test(f1, f2) VALUES(1, 1)");
        stmt.executeUpdate("UPDATE test SET f2 = 2 WHERE f1 = 1");
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        Assert.assertTrue(rs.next());
        System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
        Assert.assertFalse(rs.next());
        rs.close();
        stmt.executeUpdate("DELETE FROM test WHERE f1 = 1");
        rs = stmt.executeQuery("SELECT * FROM test");
        Assert.assertFalse(rs.next());
        rs.close();
        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        stmt.close();
        conn.close();
    }
}

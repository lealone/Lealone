/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.lealone.test.TestBase;

public class EmbeddedExample {
    public static void main(String[] args) throws Exception {
        TestBase.initTransactionEngine();
        TestBase test = new TestBase();
        test.setInMemory(true);
        test.setEmbedded(true);
        test.printURL();

        Connection conn = test.getConnection();
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

        stmt.close();
        conn.close();
        TestBase.closeTransactionEngine();
    }
}

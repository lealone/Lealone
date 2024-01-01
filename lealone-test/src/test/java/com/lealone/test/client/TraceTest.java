/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.client;

import java.sql.ResultSet;

import org.junit.Test;

import com.lealone.common.trace.TraceSystem;

public class TraceTest extends ClientTestBase {

    public TraceTest() {
        super("TraceTestDB");
        enableTrace(TraceSystem.DEBUG);
    }

    @Test
    public void run() throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS TraceTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS TraceTest (f1 int, f2 long)");
        stmt.executeUpdate("INSERT INTO TraceTest(f1, f2) VALUES(1, 1)");
        ResultSet rs = stmt.executeQuery("SELECT * FROM TraceTest");
        rs.close();
    }
}
/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.client;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.junit.Test;

public class JSR310JavaTimeTest extends ClientTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS JSR310JavaTimeTest");
        stmt.executeUpdate(
                "CREATE TABLE IF NOT EXISTS JSR310JavaTimeTest (f1 date, f2 time, f3 timestamp)");

        PreparedStatement ps = conn
                .prepareStatement("insert into JSR310JavaTimeTest(f1,f2,f3) values(?,?,?)");

        LocalDate date = LocalDate.now();
        LocalTime time = LocalTime.now();
        LocalDateTime dateTime = LocalDateTime.now();
        ps.setObject(1, date);
        ps.setObject(2, time);
        ps.setObject(3, dateTime);
        ps.executeUpdate();
        ps.close();

        ResultSet rs = stmt.executeQuery("SELECT f1,f2,f3 FROM JSR310JavaTimeTest");
        assertTrue(rs.next());
        assertEquals(date, rs.getObject(1, LocalDate.class));
        assertEquals(time, rs.getObject(2, LocalTime.class));
        assertEquals(dateTime, rs.getObject(3, LocalDateTime.class));
        rs.close();
    }
}

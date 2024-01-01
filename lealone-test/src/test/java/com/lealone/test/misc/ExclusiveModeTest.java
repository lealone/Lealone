/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.misc;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class ExclusiveModeTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        final Connection conn1 = getConnection();
        final Connection conn2 = getConnection();

        // Thread-1使用了排它模式，只有它运行完时，才到Thread-2
        Thread t1 = new Thread(() -> {
            try {
                Statement stmt = conn1.createStatement();
                stmt.executeUpdate("set EXCLUSIVE 1");
                Thread.sleep(2000);
                stmt.close();
                conn1.close();
                System.out.println("Thread-1 end");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            try {
                Thread.sleep(1000);
                Statement stmt = conn2.createStatement();
                stmt.executeQuery("select 1");
                stmt.close();
                conn2.close();
                System.out.println("Thread-2 end");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }
}

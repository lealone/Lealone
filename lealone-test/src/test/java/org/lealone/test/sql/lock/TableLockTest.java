/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.sql.lock;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class TableLockTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("set DEFAULT_LOCK_TIMEOUT 2000");
        stmt.executeUpdate("drop table IF EXISTS TableLockTest");
        stmt.executeUpdate("create table IF NOT EXISTS TableLockTest(id int, f1 int)");
        stmt.executeUpdate("insert into TableLockTest(id, f1) values(1, 2)");

        // 如果t1先执行executeUpdate，那么t2和t3需要等待t1提交了才能执行，
        // 如果t2或t3其中之一先执行了，t1也需要等待，t2和t3可以同时执行
        // 2021-03-18更新:
        // t1、t2、t3都可以同时执行了
        Thread t1 = new Thread(() -> {
            try {
                Connection conn = TableLockTest.this.getConnection();
                conn.setAutoCommit(false);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("TRUNCATE TABLE TableLockTest"); // 会锁表
                conn.commit();
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            try {
                Connection conn = TableLockTest.this.getConnection();
                conn.setAutoCommit(false);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("insert into TableLockTest(id, f1) values(2, 3)");
                conn.commit();
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread t3 = new Thread(() -> {
            try {
                Connection conn = TableLockTest.this.getConnection();
                conn.setAutoCommit(false);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("insert into TableLockTest(id, f1) values(3, 4)");
                conn.commit();
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
    }
}

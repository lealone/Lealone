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
package org.lealone.test.misc.lock;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class DeadlockTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("set DEFAULT_LOCK_TIMEOUT 2000");
        stmt.executeUpdate("drop table IF EXISTS DeadlockTest1");
        stmt.executeUpdate("drop table IF EXISTS DeadlockTest2");
        stmt.executeUpdate("create table IF NOT EXISTS DeadlockTest1(id int, name varchar(500), b boolean)");
        stmt.executeUpdate("create table IF NOT EXISTS DeadlockTest2(id int, name varchar(500), b boolean)");

        stmt.executeUpdate("insert into DeadlockTest1(id, name, b) values(1, 'a1', true)");
        stmt.executeUpdate("insert into DeadlockTest2(id, name, b) values(1, 'a1', true)");

        Thread t1 = new Thread(() -> {
            try {
                Connection conn = DeadlockTest.this.getConnection();
                conn.setAutoCommit(false);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("update DeadlockTest1 set name = 'a2' where id = 1");
                stmt.executeUpdate("update DeadlockTest2 set name = 'a2' where id = 1");
                conn.commit();
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            try {
                Connection conn = DeadlockTest.this.getConnection();
                conn.setAutoCommit(false);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("update DeadlockTest2 set name = 'a2' where id = 1");
                stmt.executeUpdate("update DeadlockTest1 set name = 'a2' where id = 1");
                conn.commit();
                stmt.close();
                conn.close();
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

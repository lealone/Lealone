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
package org.lealone.test.replication;

import java.sql.ResultSet;

import org.junit.Test;
import org.lealone.db.RunMode;
import org.lealone.test.sql.SqlTestBase;

public class ReplicationTest extends SqlTestBase {

    private static final String REPLICATION_DB_NAME = "ReplicationTestDB";

    public ReplicationTest() {
        super(REPLICATION_DB_NAME, RunMode.REPLICATION); // 自动创建一个使用复制模式的ReplicationTestDB数据库
        // setHost("127.0.0.1");
    }

    @Test
    public void run() throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS ReplicationTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ReplicationTest (f1 int primary key, f2 long)");
        stmt.executeUpdate("INSERT INTO ReplicationTest(f1, f2) VALUES(1, 2)");

        // 启动两个新事务更新同一行，可以用来测试Replication冲突的场景
        Thread t1 = new Thread(new CrudTest());
        Thread t2 = new Thread(new CrudTest());
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        ResultSet rs = stmt.executeQuery("SELECT f1, f2 FROM ReplicationTest");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(20, rs.getLong(2));
        rs.close();
        stmt.executeUpdate("DELETE FROM ReplicationTest WHERE f1 = 1");
    }

    private static class CrudTest extends SqlTestBase implements Runnable {

        public CrudTest() {
            super(REPLICATION_DB_NAME);
        }

        @Override
        protected void test() throws Exception {
            // conn.setAutoCommit(false);
            stmt.executeUpdate("update ReplicationTest set f2 = 20 where f1 = 1");
            // conn.commit();
        }

        @Override
        public void run() {
            runTest();
        }
    }
}

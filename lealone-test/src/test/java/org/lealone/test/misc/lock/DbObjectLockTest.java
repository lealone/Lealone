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

public class DbObjectLockTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        run(true);
        run(false);
    }

    private void run(final boolean isAutoCommit) throws Exception {
        Thread t1 = new Thread(() -> {
            testDbObjectLock(isAutoCommit);
        });
        Thread t2 = new Thread(() -> {
            testDbObjectLock(isAutoCommit);
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    private void testDbObjectLock(boolean isAutoCommit) {
        try {
            Connection conn = getConnection();
            if (!isAutoCommit)
                conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("set LOCK_TIMEOUT 200000");
            stmt.executeUpdate("create table IF NOT EXISTS DbObjectLockTest(id int, name varchar(500))");
            if (!isAutoCommit)
                conn.commit();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

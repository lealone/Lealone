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
package org.lealone.test.sql.transaction;

import static junit.framework.Assert.assertEquals;

import java.sql.Savepoint;

import org.junit.Test;
import org.lealone.test.TestBase;

public class TransactionTest extends TestBase {

    @Test
    public void run() throws Exception {
        create();
        //        insert();
        //        select();

        testCommit();
        //testRollback();
        //testSavepoint();
    }

    void create() throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS TransactionTest");
        //stmt.executeUpdate("CREATE TABLE IF NOT EXISTS TransactionTest (f1 int NOT NULL PRIMARY KEY, f2 int, f3 varchar)");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS TransactionTest (SPLIT KEYS('200'),f1 int NOT NULL PRIMARY KEY, f2 int, f3 varchar)");
        //        stmt.executeUpdate("CREATE PRIMARY KEY HASH IF NOT EXISTS TransactionTest_idx1 ON TransactionTest(f1)");
        //        stmt.executeUpdate("CREATE UNIQUE HASH INDEX IF NOT EXISTS TransactionTest_idx2 ON TransactionTest(f2)");
        //        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS TransactionTest_idx3 ON TransactionTest(f3, f2)");
    }

    void delete() throws Exception {
        stmt.executeUpdate("DELETE FROM TransactionTest");
    }

    void insert() throws Exception {
        //delete();

        stmt.executeUpdate("INSERT INTO TransactionTest(f3, f2, f1) VALUES('d', 40, 400)");
        stmt.executeUpdate("INSERT INTO TransactionTest(f1, f2, f3) VALUES(100, 10, 'a')");
        stmt.executeUpdate("INSERT INTO TransactionTest(f1, f2, f3) VALUES(200, 20, 'b')");
        stmt.executeUpdate("INSERT INTO TransactionTest(f1, f2, f3) VALUES(300, 30, 'c')");
        //        try {
        //            stmt.executeUpdate("INSERT INTO TransactionTest(f1, f2, f3) VALUES(400, 20, 'd')");
        //            Assert.fail("insert duplicate key: 20");
        //        } catch (SQLException e) {
        //            //e.printStackTrace();
        //        }
        //
        //        try {
        //            stmt.executeUpdate("INSERT INTO TransactionTest(f1, f2, f3) VALUES(500, 20, 'e')");
        //            Assert.fail("insert duplicate key: 20");
        //        } catch (SQLException e) {
        //            //e.printStackTrace();
        //        }
        //
        //        try {
        //            stmt.executeUpdate("INSERT INTO TransactionTest(f1, f2, f3) VALUES(600, 20, 'f')");
        //            Assert.fail("insert duplicate key: 20");
        //        } catch (SQLException e) {
        //            //e.printStackTrace();
        //        }
    }

    void testCommit() throws Exception {
        try {
            conn.setAutoCommit(false);
            insert();
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }

        sql = "SELECT f1, f2, f3 FROM TransactionTest";
        //printResultSet();

        sql = "SELECT count(*) FROM TransactionTest";
        assertEquals(4, getIntValue(1, true));

        sql = "DELETE FROM TransactionTest";
        assertEquals(4, stmt.executeUpdate(sql));

        sql = "SELECT count(*) FROM TransactionTest";
        assertEquals(0, getIntValue(1, true));
    }

    void testRollback() throws Exception {
        try {
            conn.setAutoCommit(false);
            insert();
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }

        sql = "SELECT count(*) FROM TransactionTest";
        assertEquals(0, getIntValue(1, true));

    }

    void select() throws Exception {
        sql = "SELECT f1, f2, f3 FROM TransactionTest";
        printResultSet();

        sql = "SELECT count(*) FROM TransactionTest";
        assertEquals(4, getIntValue(1, true));

        sql = "SELECT f1, f2, f3 FROM TransactionTest WHERE f1 >= 200";
        printResultSet();

        sql = "SELECT count(*) FROM TransactionTest WHERE f1 >= 200";
        assertEquals(3, getIntValue(1, true));

        sql = "SELECT f1, f2, f3 FROM TransactionTest WHERE f2 >= 20";
        printResultSet();

        sql = "SELECT count(*) FROM TransactionTest WHERE f2 >= 20";
        assertEquals(3, getIntValue(1, true));

        sql = "SELECT f1, f2, f3 FROM TransactionTest WHERE f3 >= 'b' AND f3 <= 'c'";
        printResultSet();

        sql = "SELECT count(*) FROM TransactionTest WHERE f3 >= 'b' AND f3 <= 'c'";
        assertEquals(2, getIntValue(1, true));

        sql = "DELETE FROM TransactionTest WHERE f2 >= 20";
        assertEquals(3, stmt.executeUpdate(sql));
    }

    void testSavepoint() throws Exception {
        stmt.executeUpdate("DELETE FROM TransactionTest");
        try {
            conn.setAutoCommit(false);
            stmt.executeUpdate("INSERT INTO TransactionTest(f1, f2, f3) VALUES(100, 10, 'a')");
            stmt.executeUpdate("INSERT INTO TransactionTest(f1, f2, f3) VALUES(200, 20, 'b')");
            Savepoint savepoint = conn.setSavepoint();
            stmt.executeUpdate("INSERT INTO TransactionTest(f1, f2, f3) VALUES(300, 30, 'c')");
            sql = "SELECT f1, f2, f3 FROM TransactionTest";
            //printResultSet();
            conn.rollback(savepoint);
            //调用rollback(savepoint)后还是需要调用commit
            conn.commit();
            //或调用rollback也能撤消之前的操作
            //conn.rollback();
        } finally {
            //这个内部也会触发commit
            conn.setAutoCommit(true);
        }

        sql = "SELECT f1, f2, f3 FROM TransactionTest";
        //printResultSet();
    }
}

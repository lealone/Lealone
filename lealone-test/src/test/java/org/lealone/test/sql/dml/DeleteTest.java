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
package org.lealone.test.sql.dml;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class DeleteTest extends SqlTestBase {
    @Test
    public void run() {
        createTable("DeleteTest");
        testInsert();
        testDelete();
        testRowLock();
    }

    void testInsert() {
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('01', 'a1', 'b', 51)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 61)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('03', 'a1', 'b', 61)");

        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('25', 'a2', 'b', 51)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('26', 'a2', 'b', 61)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('27', 'a2', 'b', 61)");

        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('50', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('51', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('52', 'a1', 'b', 12)");

        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('75', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('76', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('77', 'a1', 'b', 12)");
    }

    void testDelete() {
        sql = "DELETE FROM DeleteTest WHERE pk = '01'";
        assertEquals(1, executeUpdate(sql));

        sql = "DELETE FROM DeleteTest WHERE pk <= '25'";
        assertEquals(3, executeUpdate(sql));

        sql = "DELETE FROM DeleteTest WHERE pk = '26'";
        assertEquals(1, executeUpdate(sql));

        sql = "DELETE FROM DeleteTest WHERE pk > '25' AND pk < '50'";
        assertEquals(1, executeUpdate(sql));

        sql = "DELETE FROM DeleteTest WHERE pk >= '50'";
        assertEquals(6, executeUpdate(sql));

        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('101', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('102', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('103', 'a1', 'b', 12)");

        sql = "DELETE TOP 1 FROM DeleteTest";
        assertEquals(1, executeUpdate(sql));

        sql = "DELETE FROM DeleteTest LIMIT 2";
        assertEquals(2, executeUpdate(sql));
    }

    void testRowLock() {
        try {
            executeUpdate("INSERT INTO DeleteTest(pk, f1, f2, f3) VALUES('100', 'a1', 'b', 51)");
            conn.setAutoCommit(false);
            sql = "UPDATE DeleteTest SET f1 = 'a2' WHERE pk = '100'";
            executeUpdate(sql);

            Connection conn2 = null;
            try {
                conn2 = getConnection();
                conn2.setAutoCommit(false);
                Statement stmt2 = conn2.createStatement();
                stmt2.executeUpdate("SET LOCK_TIMEOUT = 300");
                stmt2.executeUpdate("DELETE FROM DeleteTest WHERE pk = '100'");
                fail();
            } catch (Exception e) {
                UpdateTest.assertLockTimeout(conn2, e);
            }

            conn.commit();
            conn.setAutoCommit(true);

            sql = "SELECT f1, f2, f3 FROM DeleteTest WHERE pk = '100'";
            assertEquals("a2", getStringValue(1, true));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            conn.setAutoCommit(false);
            sql = "DELETE FROM DeleteTest WHERE pk = '100'";
            executeUpdate(sql);

            Connection conn2 = null;
            try {
                conn2 = getConnection();
                conn2.setAutoCommit(false);
                Statement stmt2 = conn2.createStatement();
                stmt2.executeUpdate("SET LOCK_TIMEOUT = 300");
                stmt2.executeUpdate("UPDATE DeleteTest SET f1 = 'a2' WHERE pk = '100'");
                fail();
            } catch (Exception e) {
                UpdateTest.assertLockTimeout(conn2, e);
            }

            conn.commit();
            conn.setAutoCommit(true);

            sql = "SELECT count(*) FROM DeleteTest WHERE pk = '100'";
            assertEquals(0, getIntValue(1, true));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

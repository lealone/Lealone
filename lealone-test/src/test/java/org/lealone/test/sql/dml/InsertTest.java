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

public class InsertTest extends SqlTestBase {
    @Test
    public void run() {
        createTable("InsertTest");
        createTable("InsertTest2");
        testInsert();
        testInsertFromSelect();
        testDuplicateKey();
        testUnique();
        testPrimaryKeyUniqueIndex();
    }

    // 测试short/int/long类型的primary key + unique约束字段构成的索引
    void testPrimaryKeyUniqueIndex() {
        executeUpdate("DROP TABLE IF EXISTS testUnique");
        executeUpdate("CREATE TABLE testUnique (pk int PRIMARY KEY, f1 int UNIQUE)");
        Thread t1 = startThread("INSERT INTO testUnique(pk, f1) VALUES(100, 10)");
        Thread t2 = startThread("INSERT INTO testUnique(pk, f1) VALUES(100, 10)");
        Thread t3 = startThread("INSERT INTO testUnique(pk, f1) VALUES(100, 10)");
        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void testUnique() {
        executeUpdate("DROP TABLE IF EXISTS testUnique");
        executeUpdate("CREATE TABLE testUnique (pk varchar(100) PRIMARY KEY, f1 int)");
        Thread t1 = startThread("INSERT INTO testUnique(pk, f1) VALUES('a', 10)");
        Thread t2 = startThread("INSERT INTO testUnique(pk, f1) VALUES('a', 20)");
        Thread t3 = startThread("INSERT INTO testUnique(pk, f1) VALUES('a', 30)");
        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void testDuplicateKey() {
        executeUpdate("DROP TABLE IF EXISTS testDuplicateKey");
        executeUpdate("CREATE TABLE testDuplicateKey (pk int PRIMARY KEY, f1 int)");
        Thread t1 = startThread("INSERT INTO testDuplicateKey(pk, f1) VALUES(1, 10)");
        Thread t2 = startThread("INSERT INTO testDuplicateKey(pk, f1) VALUES(1, 20)");
        Thread t3 = startThread("INSERT INTO testDuplicateKey(pk, f1) VALUES(1, 30)");
        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    Thread startThread(String sql) {
        Thread t = new Thread(() -> {
            try {
                Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                stmt.executeUpdate(sql);
                stmt.close();
                conn.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        });
        t.start();
        return t;
    }

    void testInsert() {
        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('01', 'a1', 'b', 51)");
        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('02', 'a1', 'b', 61)");
        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('03', 'a1', 'b', 61)");

        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('25', 'a2', 'b', 51)");
        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('26', 'a2', 'b', 61)");
        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('27', 'a2', 'b', 61)");

        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('50', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('51', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('52', 'a1', 'b', 12)");

        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('75', 'a1', 'b', 12)");
        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('76', 'a2', 'b', 12)");
        executeUpdate("INSERT INTO InsertTest(pk, f1, f2, f3) VALUES('77', 'a1', 'b', 12)");

        sql = "DELETE FROM InsertTest";
        assertEquals(12, executeUpdate(sql));

        sql = "INSERT INTO InsertTest(pk, f1, f2, f3) VALUES"
                + " ('01', 'a1', 'b', 12), ('02', 'a1', 'b', 12), ('03', 'a1', 'b', 12)"
                + ",('25', 'a1', 'b', 12), ('26', 'a1', 'b', 12), ('27', 'a1', 'b', 12)"
                + ",('50', 'a1', 'b', 12), ('51', 'a1', 'b', 12), ('52', 'a1', 'b', 12)"
                + ",('75', 'a1', 'b', 12), ('76', 'a1', 'b', 12), ('77', 'a1', 'b', 12)";

        assertEquals(12, executeUpdate(sql));

        sql = "DELETE FROM InsertTest";
        assertEquals(12, executeUpdate(sql));

        // test NULL_TO_DEFAULT
        sql = "DROP TABLE IF EXISTS InsertTest3";
        executeUpdate(sql);
        sql = "CREATE TABLE IF NOT EXISTS InsertTest3 (f1 int, f2 int NULL_TO_DEFAULT)";
        executeUpdate(sql);
        executeUpdate("insert into InsertTest3(f1) values(10)");
        sql = "DELETE FROM InsertTest3";
        assertEquals(1, executeUpdate(sql));
    }

    void testInsertFromSelect() {
        sql = "INSERT INTO InsertTest2(pk, f1, f2, f3) VALUES"
                + " ('01', 'a1', 'b', 12), ('02', 'a1', 'b', 12), ('03', 'a1', 'b', 12)"
                + ",('25', 'a1', 'b', 12), ('26', 'a1', 'b', 12), ('27', 'a1', 'b', 12)"
                + ",('50', 'a1', 'b', 12), ('51', 'a1', 'b', 12), ('52', 'a1', 'b', 12)"
                + ",('75', 'a1', 'b', 12), ('76', 'a1', 'b', 12), ('77', 'a1', 'b', 12)";

        assertEquals(12, executeUpdate(sql));

        sql = "INSERT INTO InsertTest(pk, f1, f2, f3) SELECT pk, f1, f2, f3 FROM InsertTest2";
        assertEquals(12, executeUpdate(sql));

        sql = "DELETE FROM InsertTest";
        assertEquals(12, executeUpdate(sql));

        sql = "INSERT INTO InsertTest(pk, f1, f2, f3) " + "DIRECT SELECT pk, f1, f2, f3 FROM InsertTest2";
        assertEquals(12, executeUpdate(sql));

        sql = "DELETE FROM InsertTest";
        assertEquals(12, executeUpdate(sql));

        sql = "INSERT INTO InsertTest(pk, f1, f2, f3) " + " SELECT pk, f1, f2, f3 FROM InsertTest2 WHERE pk='01'";
        assertEquals(1, executeUpdate(sql));
    }
}

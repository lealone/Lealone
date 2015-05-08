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
package org.lealone.test.misc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.lealone.test.sql.TestBase;

public class WiredTigerExample {
    static Connection getConnection() throws Exception {
        String url = "jdbc:lealone:tcp://localhost:5210/" + TestBase.db;
        url = "jdbc:lealone:embed:./lealone-test-data/" + TestBase.db;

        return DriverManager.getConnection(url, "sa", "");
    }

    public static void main(String[] args) throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();

        try {
            stmt.executeUpdate("DROP TABLE IF EXISTS test");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long) engine wt");
            stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_test_f2 ON test(f2)");

            for (int i = 1; i <= 10; i++) {
                stmt.executeUpdate("INSERT INTO test(f1, f2) VALUES(" + i + "," + i * 10 + ")");
            }

            String sql = "SELECT * FROM test WHERE f1 = 3";
            sql = "SELECT * FROM test";
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
            }
            rs.close();

            stmt.executeUpdate("UPDATE test SET f2 = 1 WHERE f1 = 1");

            //stmt.executeUpdate("UPDATE test SET f2 = 1 WHERE f1 >= 2");

            //rs = stmt.executeQuery("SELECT * FROM test WHERE f1 <= 3");
            rs = stmt.executeQuery("SELECT * FROM test WHERE f1 = 3");
            while (rs.next()) {
                System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
            }

            rs.close();
            stmt.executeUpdate("DELETE FROM test WHERE f1 = 1");

            rs = stmt.executeQuery("SELECT count(*) FROM test");
            while (rs.next()) {
                System.out.println("count=" + rs.getInt(1));
            }

            rs.close();

            rs = stmt.executeQuery("SELECT count(*) FROM test WHERE f2=20");
            while (rs.next()) {
                System.out.println("count=" + rs.getInt(1));
            }

            rs.close();
        } finally {
            stmt.close();
            conn.close();
        }
    }
}

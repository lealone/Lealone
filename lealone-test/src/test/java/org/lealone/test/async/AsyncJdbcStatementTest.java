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
package org.lealone.test.async;

import java.sql.Connection;
import java.sql.ResultSet;

import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.test.TestBase;

public class AsyncJdbcStatementTest {

    public static void main(String[] args) throws Exception {
        Connection conn = new TestBase().getConnection();
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");
        String sql = "INSERT INTO test(f1, f2) VALUES(1, 2)";
        stmt.executeUpdate(sql);

        stmt.executeUpdateAsync("INSERT INTO test(f1, f2) VALUES(2, 2)", res -> {
            System.out.println("updateCount: " + res.getResult());

            try {
                stmt.executeQueryAsync("SELECT * FROM test where f2 = 2", res2 -> {
                    ResultSet rs = res2.getResult();
                    try {
                        while (rs.next()) {
                            System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                        }
                        stmt.close();
                        conn.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}

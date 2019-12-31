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
import java.sql.SQLException;

import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.TestBase;

public class AsyncJdbcStatementTest {

    public static void main(String[] args) throws Exception {
        Connection conn = new TestBase().getConnection(LealoneDatabase.NAME);
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        // stmt.executeUpdate("DROP TABLE IF EXISTS test");

        stmt.executeUpdateAsync("DROP TABLE IF EXISTS test").onSuccess(updateCount -> {
            System.out.println("updateCount: " + updateCount);
        }).onFailure(e -> {
            e.printStackTrace();
        }).get();

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)");
        String sql = "INSERT INTO test(f1, f2) VALUES(1, 2)";
        stmt.executeUpdate(sql);

        stmt.executeUpdateAsync("INSERT INTO test(f1, f2) VALUES(3, 3)").onSuccess(updateCount -> {
            System.out.println("updateCount: " + updateCount);
        }).onFailure(e -> {
            e.printStackTrace();
        }).get();

        stmt.executeUpdateAsync("INSERT INTO test(f1, f2) VALUES(2, 2)", res -> {
            if (res.isSucceeded()) {
                System.out.println("updateCount: " + res.getResult());
            } else {
                close(stmt, conn);
                res.getCause().printStackTrace();
                return;
            }

            try {
                stmt.executeQueryAsync("SELECT * FROM test where f2 = 2", res2 -> {
                    if (res2.isSucceeded()) {
                        ResultSet rs = res2.getResult();
                        try {
                            while (rs.next()) {
                                System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                            }
                            close(stmt, conn);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        close(stmt, conn);
                        res2.getCause().printStackTrace();
                        return;
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    static void close(JdbcStatement stmt, Connection conn) {
        try {
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

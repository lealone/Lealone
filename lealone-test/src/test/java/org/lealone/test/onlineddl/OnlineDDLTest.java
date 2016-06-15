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
package org.lealone.test.onlineddl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Assert;
import org.lealone.test.TestBase;

public class OnlineDDLTest {
    public static void main(String[] args) throws Exception {
        Connection conn = new TestBase().getConnection();
        // Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9092/test;user=sa;password=");
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("CREATE SCHEMA IF NOT EXISTS schema_test");
            stmt.executeUpdate("USE schema_test");
            stmt.executeUpdate("DROP TABLE IF EXISTS test CASCADE");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long, f3 int, f_blob blob)");
            stmt.executeUpdate("CREATE VIEW IF NOT EXISTS test_view (v_f1,v_f2) AS SELECT f1,f2 FROM test");
            // stmt.executeUpdate("CREATE FORCE TRIGGER IF NOT EXISTS test_trigger"
            // + " BEFORE INSERT,UPDATE,DELETE,SELECT,ROLLBACK ON test"
            // + " QUEUE 10 NOWAIT CALL \"org.lealone.test.db.schema.TriggerObjectTest$MyTrigger\"");

            stmt.executeUpdate("ALTER TABLE test ADD CONSTRAINT test_constraint_check CHECK (f1 > 1)");
            stmt.executeUpdate("ALTER TABLE test ADD CONSTRAINT test_constraint_unique UNIQUE KEY (f2)");
            stmt.executeUpdate("DROP TABLE IF EXISTS ConstraintReferentialTestTable CASCADE");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ConstraintReferentialTestTable (f1 int PRIMARY KEY not null)");
            stmt.executeUpdate("ALTER TABLE test ADD CONSTRAINT test_constraint_referential "
                    + "FOREIGN KEY (f3) REFERENCES ConstraintReferentialTestTable(f1)");

            stmt.executeUpdate("GRANT SELECT,DELETE,INSERT ON test TO PUBLIC");

            stmt.executeUpdate("CREATE SEQUENCE IF NOT EXISTS test_sequence START WITH 1000 INCREMENT BY 1 CACHE 20");

            stmt.executeUpdate("INSERT INTO ConstraintReferentialTestTable(f1) VALUES(3)");
            stmt.executeUpdate("INSERT INTO test(f1, f2, f3) VALUES(2, 2, 3)");
            stmt.executeUpdate("INSERT INTO test(f1, f2, f3) VALUES(6, 6, 3)");
            stmt.executeUpdate("ALTER TABLE test ADD COLUMN f4 int AUTO_INCREMENT");

            ResultSet rs = stmt.executeQuery("SELECT * FROM test");
            Assert.assertTrue(rs.next());
            System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2) + " f4=" + rs.getLong("f4"));
            Assert.assertTrue(rs.next());
            System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2) + " f4=" + rs.getLong("f4"));

            stmt.executeUpdate("ALTER TABLE test ADD COLUMN f5 int SEQUENCE test_sequence");

            stmt.executeUpdate("ALTER TABLE test ADD COLUMN f6 int BEFORE f5");

            stmt.executeUpdate("ALTER TABLE test DROP COLUMN f4");

            rs = stmt.executeQuery("SELECT * FROM test");
            Assert.assertTrue(rs.next());
            try {
                System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                rs.getLong("f4");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            Assert.assertTrue(rs.next());
            try {
                System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                rs.getLong("f4");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

            stmt.executeUpdate("CREATE INDEX test_index ON test(f5)");

            // stmt.executeUpdate("INSERT INTO test(f1, f2) VALUES(1, 1)");
            // stmt.executeUpdate("UPDATE test SET f2 = 2 WHERE f1 = 1");
            // ResultSet rs = stmt.executeQuery("SELECT * FROM test");
            // Assert.assertTrue(rs.next());
            // System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
            // Assert.assertFalse(rs.next());
            // rs.close();
            // stmt.executeUpdate("DELETE FROM test WHERE f1 = 1");
            // rs = stmt.executeQuery("SELECT * FROM test");
            // Assert.assertFalse(rs.next());
            // rs.close();
            stmt.executeUpdate("DROP TABLE IF EXISTS test CASCADE");
            // stmt.executeUpdate("DROP SCHEMA IF EXISTS schema_test");
            stmt.close();
        } finally {
            conn.close();
        }
    }
}

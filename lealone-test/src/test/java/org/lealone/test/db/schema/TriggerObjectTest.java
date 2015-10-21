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
package org.lealone.test.db.schema;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;
import org.lealone.api.ErrorCode;
import org.lealone.api.Trigger;
import org.lealone.db.result.Result;
import org.lealone.test.db.DbObjectTestBase;

public class TriggerObjectTest extends DbObjectTestBase {

    public static class MyTrigger implements Trigger {

        @Override
        public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before,
                int type) throws SQLException {
            System.out.println("schemaName=" + schemaName + " tableName=" + tableName);

        }

        @Override
        public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
            System.out.println("oldRow=" + oldRow + " newRow=" + newRow);
        }

        @Override
        public void close() throws SQLException {
            System.out.println("org.lealone.test.db.schema.TriggerObjectTest.MyInsertTrigger.close()");
        }

        @Override
        public void remove() throws SQLException {
            System.out.println("org.lealone.test.db.schema.TriggerObjectTest.MyInsertTrigger.remove()");
        }

    }

    @Test
    public void run() throws Exception {
        create();
        drop();
    }

    void create() {
        session.setAutoCommit(false);

        executeUpdate("DROP TABLE IF EXISTS CreateTriggerTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS CreateTriggerTest(id int, name varchar(500))");

        executeUpdate("CREATE FORCE TRIGGER IF NOT EXISTS MyTrigger1"
                + " BEFORE INSERT,UPDATE,DELETE,SELECT,ROLLBACK ON CreateTriggerTest"
                + " QUEUE 10 NOWAIT CALL \"org.lealone.test.db.schema.TriggerObjectTest$MyTrigger\"");

        assertNotNull(schema.findTrigger("MyTrigger1"));

        try {
            // QUEUE不能是负数
            executeUpdate("CREATE TRIGGER IF NOT EXISTS MyTrigger2"//
                    + " AFTER INSERT ON CreateTriggerTest"
                    + " QUEUE -1 CALL \"org.lealone.test.db.schema.TriggerObjectTest$MyTrigger\"");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.INVALID_VALUE_2);
        }

        try {
            executeUpdate("CREATE TRIGGER IF NOT EXISTS MyTrigger2"
                    + " AFTER INSERT,UPDATE,DELETE,SELECT,ROLLBACK ON CreateTriggerTest FOR EACH ROW"
                    + " QUEUE 10 NOWAIT CALL \"org.lealone.test.db.schema.TriggerObjectTest$MyTrigger\"");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.TRIGGER_SELECT_AND_ROW_BASED_NOT_SUPPORTED);
        }

        // INSTEAD OF也是BEFORE类型
        executeUpdate("CREATE TRIGGER IF NOT EXISTS MyTrigger3"
                + " INSTEAD OF INSERT,UPDATE,DELETE,ROLLBACK ON CreateTriggerTest FOR EACH ROW"
                + " QUEUE 10 NOWAIT CALL \"org.lealone.test.db.schema.TriggerObjectTest$MyTrigger\"");

        // 这种语法可查入多条记录
        // null null
        // 10 a
        // 20 b
        executeUpdate("INSERT INTO CreateTriggerTest VALUES(DEFAULT, DEFAULT),(10, 'a'),(20, 'b')");

        sql = "select id,name from CreateTriggerTest";
        Result rs = executeQuery(sql);
        while (rs.next()) {
            System.out.println(getString(rs, 1) + " " + getString(rs, 2));
        }

        session.commit(false);
    }

    void drop() {
        executeUpdate("DROP TRIGGER IF EXISTS MyTrigger1");
        assertNull(schema.findTrigger("MyTrigger1"));
    }
}

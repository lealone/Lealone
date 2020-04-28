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

import org.junit.Test;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.Schema;
import org.lealone.test.db.DbObjectTestBase;

public class SchemaTest extends DbObjectTestBase {
    String userName = "sa1";
    String schemaName = "SchemaTest";
    Schema schema;
    int id;

    @Test
    public void run() {
        create();
        alter();
        drop();
    }

    void create() {
        executeUpdate("CREATE USER IF NOT EXISTS " + userName + " PASSWORD 'abc' ADMIN");

        id = db.allocateObjectId();

        Schema schema = new Schema(db, id, schemaName, db.getUser(userName), false);
        assertEquals(id, schema.getId());

        db.addDatabaseObject(session, schema);
        assertNotNull(db.findSchema(schemaName));
        assertNotNull(findMeta(id));

        db.removeDatabaseObject(session, schema);
        assertNull(db.findSchema(schemaName));
        assertNull(findMeta(id));

        // 测试SQL
        // -----------------------------------------------
        executeUpdate("CREATE SCHEMA IF NOT EXISTS " + schemaName + " AUTHORIZATION " + userName);
        schema = db.findSchema(schemaName);
        assertNotNull(schema);
        id = schema.getId();
        assertNotNull(findMeta(id));

        try {
            executeUpdate("CREATE SCHEMA " + schemaName + " AUTHORIZATION " + userName);
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SCHEMA_ALREADY_EXISTS_1);
        }

        try {
            // SchemaTest_u1需要有Right.ALTER_ANY_SCHEMA权限
            executeUpdate("CREATE USER IF NOT EXISTS SchemaTest_u1 PASSWORD 'abc'");
            executeUpdate("CREATE SCHEMA IF NOT EXISTS SchemaTest_s1 AUTHORIZATION SchemaTest_u1");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.ADMIN_RIGHTS_REQUIRED);
        } finally {
            executeUpdate("DROP USER IF EXISTS SchemaTest_u1");
        }
    }

    void alter() {
        executeUpdate("CREATE SCHEMA IF NOT EXISTS SchemaTest_s1 AUTHORIZATION " + userName);

        try {
            // 不能RENAME INFORMATION_SCHEMA(system Schema)
            executeUpdate("ALTER SCHEMA INFORMATION_SCHEMA RENAME TO SchemaTest_u1");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1);
        }

        try {
            executeUpdate("ALTER SCHEMA SchemaTest_s1 RENAME TO " + schemaName);
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SCHEMA_ALREADY_EXISTS_1);
        }

        String schemaName = "SchemaTest_s2";
        executeUpdate("ALTER SCHEMA SchemaTest_s1 RENAME TO " + schemaName);
        assertNull(db.findSchema("SchemaTest_s1"));
        assertNotNull(db.findSchema(schemaName));
        executeUpdate("DROP SCHEMA IF EXISTS " + schemaName);
    }

    void drop() {
        executeUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        schema = db.findSchema(schemaName);
        assertNull(schema);
        assertNull(findMeta(id));
        try {
            executeUpdate("DROP SCHEMA " + schemaName);
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SCHEMA_NOT_FOUND_1);
        }

        try {
            // 不能删除system Schema
            executeUpdate("DROP SCHEMA INFORMATION_SCHEMA");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1);
        }
        executeUpdate("DROP USER IF EXISTS " + userName);
    }
}

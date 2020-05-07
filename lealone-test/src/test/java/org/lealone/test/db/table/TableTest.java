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
package org.lealone.test.db.table;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.test.db.DbObjectTestBase;

public class TableTest extends DbObjectTestBase {

    @Before
    @Override
    public void setUpBefore() {
        super.setUpBefore();
        create();
    }

    @After
    public void tearDownAfter() {
        drop();
    }

    void create() {
        executeUpdate("CREATE TABLE IF NOT EXISTS mytable (f1 int, f2 int not null,"
                + "ch varchar(10) default 'abc' CHECK length(ch)=3)");
    }

    void drop() {
        // executeUpdate("DROP TABLE IF EXISTS mytable3");
        // executeUpdate("DROP TABLE IF EXISTS mytable2");
        executeUpdate("DROP TABLE IF EXISTS mytable");
    }

    void alterColumnTest() {
        sql = "ALTER TABLE mytable ALTER COLUMN f1 RENAME TO f0";

        sql = "ALTER TABLE mytable ALTER COLUMN f1 DROP DEFAULT";
        sql = "ALTER TABLE mytable ALTER COLUMN f2 DROP NOT NULL";

        sql = "ALTER TABLE mytable ALTER COLUMN f1 TYPE long";
        sql = "ALTER TABLE mytable ALTER COLUMN f1 SET DATA TYPE long";

        sql = "ALTER TABLE mytable ALTER COLUMN f1 SET NULL";
        sql = "ALTER TABLE mytable ALTER COLUMN f1 SET NOT NULL";
        sql = "ALTER TABLE mytable ALTER COLUMN f1 SET DEFAULT 100";

        sql = "ALTER TABLE mytable ALTER COLUMN f1 TYPE int AUTO_INCREMENT";
        executeUpdate(sql);

        sql = "ALTER TABLE mytable ALTER COLUMN f1 RESTART WITH 10";
        executeUpdate(sql);

        sql = "ALTER TABLE mytable ALTER COLUMN f2 SELECTIVITY 20";
        executeUpdate(sql);
    }

    // ALTER TABLE命令就分下面5大类:
    // 增加约束、增加列、重命名表、DROP约束和列、修改列
    @Test
    public void ALTER_TABLE_SET_REFERENTIAL_INTEGRITY() {
        sql = "ALTER TABLE mytable SET REFERENTIAL_INTEGRITY TRUE CHECK";
        executeUpdate(sql);
        Table table = findTable("mytable");
        assertNotNull(table);
        assertTrue(table.getCheckForeignKeyConstraints());
    }

    @Test
    public void ALTER_TABLE_RENAME() {
        try {
            sql = "ALTER TABLE mytable RENAME TO mytable";
            executeUpdate(sql);
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1);
        }

        // 加上HIDDEN就可以rename到自身，只是改了HIDDEN属性而已
        sql = "ALTER TABLE mytable RENAME TO mytable HIDDEN";
        executeUpdate(sql);

        sql = "ALTER TABLE mytable RENAME TO mytable2 HIDDEN";
        executeUpdate(sql);
        Table table = findTable("mytable2");
        assertNotNull(table);
        table = findTable("mytable");
        assertNull(table);

        sql = "ALTER TABLE mytable2 RENAME TO mytable";
        executeUpdate(sql);
        table = findTable("mytable");
        assertNotNull(table);
        table = findTable("mytable2");
        assertNull(table);
    }

    @Test
    public void ALTER_TABLE_ALTER_COLUMN_RENAME() {
        Table table = findTable("mytable");
        assertNotNull(table);
        Column column = table.getColumn("ch");
        assertNotNull(column);

        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c_ch CHECK length(ch)=3";
        executeUpdate(sql);

        sql = "ALTER TABLE mytable ALTER COLUMN ch RENAME TO ch1";
        executeUpdate(sql);

        column = table.getColumn("ch1");
        assertNotNull(column);
        try {
            column = table.getColumn("ch");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.COLUMN_NOT_FOUND_1);
        }
    }

    @Test
    public void ALTER_TABLE_ALTER_COLUMN_NOT_NULL() {
        Table table = findTable("mytable");
        assertNotNull(table);
        Column column = table.getColumn("f1");
        assertNotNull(column);
        assertTrue(column.isNullable());

        executeUpdate("INSERT INTO mytable(f1, f2) VALUES(null, 2)");
        try {
            executeUpdate("ALTER TABLE mytable ALTER COLUMN f1 SET NOT NULL");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1);
        }

        executeUpdate("delete from mytable where f1 IS NULL");
        executeUpdate("ALTER TABLE mytable ALTER COLUMN f1 SET NOT NULL");
        assertFalse(column.isNullable());
    }

    @Test
    public void ALTER_TABLE_ALTER_COLUMN_NULL() {
        Table table = findTable("mytable");
        assertNotNull(table);
        Column column = table.getColumn("f2");
        assertNotNull(column);
        assertFalse(column.isNullable());

        executeUpdate("CREATE PRIMARY KEY IF NOT EXISTS f2_pk0 ON mytable(f2)");
        try {
            executeUpdate("ALTER TABLE mytable ALTER COLUMN f2 SET NULL");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.COLUMN_IS_PART_OF_INDEX_1);
            executeUpdate("DROP INDEX IF EXISTS f2_pk0");
        }

        executeUpdate("CREATE HASH INDEX IF NOT EXISTS f2_hash0 ON mytable(f2)");
        try {
            executeUpdate("ALTER TABLE mytable ALTER COLUMN f2 SET NULL");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.COLUMN_IS_PART_OF_INDEX_1);
            executeUpdate("DROP INDEX IF EXISTS f2_hash0");
        }

        sql = "ALTER TABLE mytable ALTER COLUMN f2 SET NULL";
        executeUpdate(sql);
        assertTrue(column.isNullable());
    }

    @Test
    public void ALTER_TABLE_ALTER_COLUMN_DEFAULT() {
        sql = "ALTER TABLE mytable ALTER COLUMN f2 TYPE int AUTO_INCREMENT";
        executeUpdate();
        sql = "ALTER TABLE mytable ALTER COLUMN f2 SET DEFAULT 100";
        executeUpdate();

        Table table = findTable("mytable");
        assertNotNull(table);
        Column column = table.getColumn("f2");
        assertNotNull(column);
        assertEquals(100, column.getDefaultExpression().getValue(session).getInt());

        executeUpdate("DROP SEQUENCE IF EXISTS myseq");
        executeUpdate("CREATE SEQUENCE IF NOT EXISTS myseq START WITH 1000 INCREMENT BY 1 CACHE 20");

        sql = "ALTER TABLE mytable ADD COLUMN f6 int SEQUENCE myseq";
        executeUpdate();

        sql = "ALTER TABLE mytable ADD COLUMN f7 int SEQUENCE myseq";
        executeUpdate();

        sql = "ALTER TABLE mytable ALTER COLUMN f6 SET DEFAULT 100";
        executeUpdate();
        column = table.getColumn("f6");
        assertNotNull(column);
        assertEquals(100, column.getDefaultExpression().getValue(session).getInt());
    }

    @Test
    public void ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE() {
        sql = "ALTER TABLE mytable ALTER COLUMN ch SET DATA TYPE varchar(20)";
        executeUpdate(sql);

        Table table = findTable("mytable");
        assertNotNull(table);
        Column column = table.getColumn("ch");
        assertNotNull(column);
        assertEquals(20, column.getPrecision());

        sql = "ALTER TABLE mytable ALTER COLUMN ch SET DATA TYPE varchar(5)";
        executeUpdate(sql);

        column = table.getColumn("ch");
        assertNotNull(column);
        assertEquals(5, column.getPrecision());
    }

    @Test
    public void ALTER_TABLE_ADD_COLUMN() {
        sql = "ALTER TABLE mytable ADD (f3 int, f4 int)";
        sql = "ALTER TABLE mytable ADD COLUMN(f3 int, f4 int)";
        sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f0 int BEFORE f1";
        sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f3 int AFTER f2";
        // sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f1 int";
        // ADD COLUMN时不能加约束，比如这个是错的:
        // ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f3 int PRIMARY KEY

        // 但是要表示特殊的PRIMARY KEY约束可以加IDENTITY
        sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f3 int IDENTITY AFTER f2";
        sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f3 int AUTO_INCREMENT AFTER f2";
        executeUpdate(sql);
    }

    @Test
    public void ALTER_TABLE_DROP_COLUMN() {
        sql = "ALTER TABLE mytable DROP f1";
        executeUpdate(sql);
        sql = "ALTER TABLE mytable DROP f2";
        executeUpdate(sql);

        sql = "ALTER TABLE mytable DROP ch"; // 不能删除最后一列
        try {
            executeUpdate(sql);
            fail("not throw SQLException");
        } catch (Exception e) {
            assertTrue(e.getMessage().toLowerCase().contains("cannot drop last column"));
        }
    }

    @Test
    public void ALTER_TABLE_ALTER_COLUMN_SELECTIVITY() {
        sql = "ALTER TABLE mytable ALTER COLUMN f2 SELECTIVITY -10"; // 小于0时还是0
        executeUpdate(sql);
        sql = "ALTER TABLE mytable ALTER COLUMN f2 SELECTIVITY 20";
        executeUpdate(sql);
        sql = "ALTER TABLE mytable ALTER COLUMN f2 SELECTIVITY 120"; // 大于100时还是100
        executeUpdate(sql);
    }

    // MySQL compatibility
    @Test
    public void ALTER_TABLE_MODIFY_COLUMN() throws Exception {
        // "MODIFY COLUMN"可以简写成"MODIFY"
        sql = "ALTER TABLE mytable MODIFY f1 long";
        executeUpdate();
        sql = "ALTER TABLE mytable MODIFY COLUMN f1 int NOT NULL";
        executeUpdate();
    }

    // PostgreSQL compatibility
    @Test
    public void ALTER_TABLE_ALTER_COLUMN_TYPE() throws Exception {
        sql = "ALTER TABLE mytable ALTER f1 TYPE long";
        executeUpdate();
        sql = "ALTER TABLE mytable ALTER COLUMN f1 TYPE int NOT NULL";
        executeUpdate();
    }

    void oldALTER_TABLE_ADD_COLUMN() throws Exception {
        sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f1 int";
        executeUpdate();

        // 增加多列时不能用before
        sql = "ALTER TABLE mytable ADD (f0 int before f1, f4 int)";
        // tryExecuteUpdate();

        sql = "ALTER TABLE mytable ADD COLUMN(f5 int AUTO_INCREMENT, f6 int)";
        executeUpdate();

        sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f0 int BEFORE f1";

        sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f3 int AFTER f2";
        // sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f1 int";
        // ADD COLUMN时不能加约束，比如这个是错的:
        // ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f3 int PRIMARY KEY

        // 但是要表示特殊的PRIMARY KEY约束可以加IDENTITY
        sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f3 int IDENTITY AFTER f2";
        sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f3 int AUTO_INCREMENT AFTER f2";
        executeUpdate();

        // 测试checkDefaultReferencesTable(Expression)
        sql = "ALTER TABLE mytable ADD (f7 int, f8 int default f2*2)";
        // tryExecuteUpdate();
        sql = "ALTER TABLE mytable ADD (f7 int, f8 int default EXISTS(select f1 from mytable where f1=1))";
        // tryExecuteUpdate();

        sql = "CREATE OR REPLACE FORCE VIEW mytable_view (v_f5) " //
                + "AS SELECT f5 FROM mytable";
        executeUpdate();
        sql = "ALTER TABLE mytable DROP f5";
        // tryExecuteUpdate();
        sql = "ALTER TABLE mytable ALTER COLUMN f3 RESTART WITH 1";
        executeUpdate();
        executeUpdate("INSERT INTO mytable(f1, f2, f3) VALUES(1, 2, null)");

        executeUpdate("DROP SEQUENCE IF EXISTS myseq10");
        executeUpdate("CREATE SEQUENCE IF NOT EXISTS myseq10 START WITH 1000 INCREMENT BY 1 CACHE 20");
        sql = "ALTER TABLE mytable ADD COLUMN f10 int SEQUENCE myseq10";
        executeUpdate();

        executeUpdate("DROP INDEX IF EXISTS mytable_index0");
        executeUpdate("CREATE INDEX mytable_index0 ON mytable(f10)");

        sql = "ALTER TABLE mytable DROP COLUMN f3";
        executeUpdate();
    }
}

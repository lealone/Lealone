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
package org.lealone.test.sql.ddl;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class AlterTableAlterColumnTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS mytable_view");
        // executeUpdate("DROP TABLE IF EXISTS mytable3");
        // executeUpdate("DROP TABLE IF EXISTS mytable2");
        executeUpdate("DROP TABLE IF EXISTS mytable");

        executeUpdate("CREATE TABLE IF NOT EXISTS mytable (f1 int, f2 int not null, ch varchar(10))");
        parseAlterTable();
    }

    void parseAlterTable() throws Exception {
        // ALTER TABLE命令就分下面5大类:
        // 增加约束、增加列、重命名表、DROP约束和列、修改列
        // parseAlterTableAddConstraintIf();
        // parseAlterTableAddColumn();
        // renameTest();

        // dropTest();
        // alterColumnTest();

        // ALTER_TABLE_ALTER_COLUMN_NOT_NULL();
        // ALTER_TABLE_ALTER_COLUMN_NULL();
        // ALTER_TABLE_ALTER_COLUMN_DEFAULT();
        // ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE();
        ALTER_TABLE_ADD_COLUMN();
        // ALTER_TABLE_DROP_COLUMN();
        // ALTER_TABLE_ALTER_COLUMN_SELECTIVITY();
        // ALTER_TABLE_ALTER_COLUMN_DROP();
        // ALTER_TABLE_MODIFY_COLUMN();
        // ALTER_TABLE_ALTER_COLUMN_TYPE();
        // ALTER_TABLE_ALTER_COLUMN_SET_DATA_TYPE();
        // ALTER_TABLE_ALTER_COLUMN();
    }

    void parseAlterTableAddConstraintIf() throws Exception {

        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c0 COMMENT IS 'haha0' PRIMARY KEY HASH(f1,f2) INDEX myindex";
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c1 COMMENT IS 'haha1' INDEX myindex(f1,f2)";
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c2 COMMENT IS 'haha2' INDEX(f1,f2)";
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c3 COMMENT IS 'haha3' CHECK f1>0 and f2<10 CHECK";
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c4 COMMENT IS 'haha4' UNIQUE KEY INDEX myunique(f1,f2) NOCHECK";
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c5 COMMENT IS 'haha5' FOREIGN KEY(f1) REFERENCES(f2)";
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c6 COMMENT IS 'haha6' FOREIGN KEY(f1) REFERENCES mytable(f2)"
                + "ON DELETE CASCADE ON UPDATE RESTRICT ON DELETE NO ACTION ON UPDATE SET NULL ON DELETE SET DEFAULT NOT DEFERRABLE";

        executeUpdate();
    }

    void parseAlterTableAddColumn() throws Exception {
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
        executeUpdate();
    }

    void renameTest() throws Exception {
        sql = "ALTER TABLE mytable SET REFERENTIAL_INTEGRITY TRUE CHECK";
        sql = "ALTER TABLE mytable RENAME TO mytable2 HIDDEN";
        executeUpdate();
    }

    void dropTest() throws Exception {
        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c3 COMMENT IS 'haha3' CHECK f1>0 and f2<10 CHECK";
        executeUpdate();
        sql = "ALTER TABLE mytable DROP CONSTRAINT c3";
        executeUpdate();

        sql = "ALTER TABLE mytable ADD CONSTRAINT IF NOT EXISTS c0 COMMENT IS 'haha0' PRIMARY KEY HASH(f2) INDEX myindex";
        executeUpdate();
        sql = "ALTER TABLE mytable DROP PRIMARY KEY";
        executeUpdate();

        sql = "ALTER TABLE mytable DROP COLUMN f1";
        sql = "ALTER TABLE mytable DROP f1";
        executeUpdate();
    }

    void alterColumnTest() throws Exception {
        sql = "ALTER TABLE mytable ALTER COLUMN f1 RENAME TO f0";

        sql = "ALTER TABLE mytable ALTER COLUMN f1 DROP DEFAULT";
        sql = "ALTER TABLE mytable ALTER COLUMN f2 DROP NOT NULL";

        sql = "ALTER TABLE mytable ALTER COLUMN f1 TYPE long";
        sql = "ALTER TABLE mytable ALTER COLUMN f1 SET DATA TYPE long";

        sql = "ALTER TABLE mytable ALTER COLUMN f1 SET NULL";
        sql = "ALTER TABLE mytable ALTER COLUMN f1 SET NOT NULL";
        sql = "ALTER TABLE mytable ALTER COLUMN f1 SET DEFAULT 100";

        sql = "ALTER TABLE mytable ALTER COLUMN f1 TYPE int AUTO_INCREMENT";
        executeUpdate();

        sql = "ALTER TABLE mytable ALTER COLUMN f1 RESTART WITH 10";
        executeUpdate();

        sql = "ALTER TABLE mytable ALTER COLUMN f2 SELECTIVITY 20";
        executeUpdate();
    }

    void ALTER_TABLE_ALTER_COLUMN_NOT_NULL() throws Exception {
        executeUpdate("INSERT INTO mytable(f1, f2) VALUES(null, 2)");
        sql = "ALTER TABLE mytable ALTER COLUMN f1 SET NOT NULL";
        tryExecuteUpdate();

        executeUpdate("delete from mytable where f2=2");
        sql = "ALTER TABLE mytable ALTER COLUMN f1 SET NOT NULL";
        executeUpdate();
    }

    void ALTER_TABLE_ALTER_COLUMN_NULL() throws Exception {
        executeUpdate("CREATE PRIMARY KEY mytableindex1 ON mytable(f2)");
        sql = "ALTER TABLE mytable ALTER COLUMN f2 SET NULL";
        tryExecuteUpdate();
        executeUpdate("DROP INDEX mytableindex1");

        executeUpdate("CREATE HASH INDEX mytableindex2 ON mytable(f2)");
        sql = "ALTER TABLE mytable ALTER COLUMN f2 SET NULL";
        tryExecuteUpdate();
        executeUpdate("DROP INDEX mytableindex2");

        sql = "ALTER TABLE mytable ALTER COLUMN f2 SET NULL";
        executeUpdate();
    }

    void ALTER_TABLE_ALTER_COLUMN_DEFAULT() throws Exception {
        sql = "ALTER TABLE mytable ALTER COLUMN f2 TYPE int AUTO_INCREMENT";
        executeUpdate();
        sql = "ALTER TABLE mytable ALTER COLUMN f2 SET DEFAULT 100";
        executeUpdate();
    }

    void ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE() throws Exception {
        sql = "ALTER TABLE mytable ALTER COLUMN ch SET DATA TYPE varchar(20)";
        executeUpdate();

        sql = "ALTER TABLE mytable ALTER COLUMN ch SET DATA TYPE varchar(5)";
        executeUpdate();
    }

    void ALTER_TABLE_ADD_COLUMN() throws Exception {
        sql = "ALTER TABLE mytable ADD COLUMN IF NOT EXISTS f1 int";
        executeUpdate();

        // 增加多列时不能用before
        sql = "ALTER TABLE mytable ADD (f0 int before f1, f4 int)";
        tryExecuteUpdate();

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

        // 测试org.h2.command.ddl.AlterTableAlterColumn.checkDefaultReferencesTable(Expression)
        sql = "ALTER TABLE mytable ADD (f7 int, f8 int default f2*2)";
        tryExecuteUpdate();
        sql = "ALTER TABLE mytable ADD (f7 int, f8 int default EXISTS(select f1 from mytable where f1=1))";
        tryExecuteUpdate();

        sql = "CREATE OR REPLACE FORCE VIEW mytable_view (v_f5) " //
                + "AS SELECT f5 FROM mytable";
        executeUpdate();
        sql = "ALTER TABLE mytable DROP f5";
        tryExecuteUpdate();
        sql = "ALTER TABLE mytable ALTER COLUMN f3 RESTART WITH 1";
        executeUpdate();
        executeUpdate("INSERT INTO mytable(f1, f2, f3) VALUES(1, 2, null)");

        stmt.executeUpdate("DROP SEQUENCE IF EXISTS myseq10");
        stmt.executeUpdate("CREATE SEQUENCE IF NOT EXISTS myseq10 START WITH 1000 INCREMENT BY 1 CACHE 20");
        sql = "ALTER TABLE mytable ADD COLUMN f10 int SEQUENCE myseq10";
        executeUpdate();

        stmt.executeUpdate("DROP INDEX IF EXISTS mytable_index0");
        stmt.executeUpdate("CREATE INDEX mytable_index0 ON mytable(f10)");

        sql = "ALTER TABLE mytable DROP COLUMN f3";
        executeUpdate();

    }

    void ALTER_TABLE_DROP_COLUMN() throws Exception {
        sql = "ALTER TABLE mytable DROP COLUMN f1";
        executeUpdate();
        sql = "ALTER TABLE mytable DROP f2";
        executeUpdate();
        sql = "ALTER TABLE mytable DROP IF EXISTS f2";
        executeUpdate();
        sql = "ALTER TABLE mytable DROP f2";
        tryExecuteUpdate();
        sql = "ALTER TABLE mytable DROP ch"; // 不能删除最后一列
        tryExecuteUpdate();
    }

    void ALTER_TABLE_ALTER_COLUMN_SELECTIVITY() throws Exception {
        sql = "ALTER TABLE mytable ALTER COLUMN f2 SELECTIVITY -10"; // 小于0时还是0
        executeUpdate();
        sql = "ALTER TABLE mytable ALTER COLUMN f2 SELECTIVITY 20";
        executeUpdate();
        sql = "ALTER TABLE mytable ALTER COLUMN f2 SELECTIVITY 120"; // 大于100时还是100
        executeUpdate();
    }

    void ALTER_TABLE_ALTER_COLUMN_DROP() throws Exception {
        // "ALTER COLUMN"可以简写成"ALTER"
        sql = "ALTER TABLE mytable ALTER f1 DROP DEFAULT";
        executeUpdate();
        sql = "ALTER TABLE mytable ALTER COLUMN f1 DROP DEFAULT";
        executeUpdate();

        sql = "ALTER TABLE mytable ALTER COLUMN f1 DROP NOT NULL";
        executeUpdate();
    }

    // MySQL compatibility
    void ALTER_TABLE_MODIFY_COLUMN() throws Exception {
        // "MODIFY COLUMN"可以简写成"MODIFY"
        sql = "ALTER TABLE mytable MODIFY f1 long";
        executeUpdate();
        sql = "ALTER TABLE mytable MODIFY COLUMN f1 int NOT NULL";
        executeUpdate();
    }

    // PostgreSQL compatibility
    void ALTER_TABLE_ALTER_COLUMN_TYPE() throws Exception {
        sql = "ALTER TABLE mytable ALTER f1 TYPE long";
        executeUpdate();
        sql = "ALTER TABLE mytable ALTER COLUMN f1 TYPE int NOT NULL";
        executeUpdate();
    }

    // Derby compatibility
    void ALTER_TABLE_ALTER_COLUMN_SET_DATA_TYPE() throws Exception {
        sql = "ALTER TABLE mytable ALTER f1 SET DATA TYPE long";
        executeUpdate();
        sql = "ALTER TABLE mytable ALTER COLUMN f1 SET DATA TYPE int NOT NULL";
        executeUpdate();
    }

    void ALTER_TABLE_ALTER_COLUMN() throws Exception {
        sql = "ALTER TABLE mytable ALTER f1 long";
        executeUpdate();
        sql = "ALTER TABLE mytable ALTER COLUMN f1 int NOT NULL";
        executeUpdate();
    }
}

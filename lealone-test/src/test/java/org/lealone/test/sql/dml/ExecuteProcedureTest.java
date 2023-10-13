/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.dml;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class ExecuteProcedureTest extends SqlTestBase {
    @Test
    public void run() {
        executeUpdate("drop table IF EXISTS ExecuteProcedureTest");
        executeUpdate(
                "create table IF NOT EXISTS ExecuteProcedureTest(id int, name varchar(500), b boolean)");
        executeUpdate(
                "CREATE INDEX IF NOT EXISTS ExecuteProcedureTestIndex ON ExecuteProcedureTest(name)");

        executeUpdate("insert into ExecuteProcedureTest(id, name, b) values(1, 'a1', true)");
        executeUpdate("insert into ExecuteProcedureTest(id, name, b) values(1, 'b1', true)");
        executeUpdate("insert into ExecuteProcedureTest(id, name, b) values(2, 'a2', false)");
        executeUpdate("insert into ExecuteProcedureTest(id, name, b) values(2, 'b2', true)");
        executeUpdate("insert into ExecuteProcedureTest(id, name, b) values(3, 'a3', false)");
        executeUpdate("insert into ExecuteProcedureTest(id, name, b) values(3, 'b3', true)");

        sql = "PREPARE mytest (int, varchar2, boolean) AS insert into ExecuteProcedureTest(id, name, b) values(?, ?, ?)";
        executeUpdate(sql);

        sql = "EXECUTE mytest(4, 'b4', true)";
        executeUpdate(sql);

        sql = "select * from ExecuteProcedureTest";
        executeQuery();

        executeUpdate("DEALLOCATE PLAN mytest");
    }
}

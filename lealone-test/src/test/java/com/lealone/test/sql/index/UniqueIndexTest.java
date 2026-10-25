/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.index;

import java.sql.Connection;
import java.sql.Statement;

import org.junit.Test;

import com.lealone.db.api.ErrorCode;
import com.lealone.test.sql.SqlTestBase;

public class UniqueIndexTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS UniqueIndexTest");
        executeUpdate(
                "CREATE TABLE IF NOT EXISTS UniqueIndexTest (f1 int NOT NULL, f2 int, f3 varchar)");
        executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS UniqueIndexTest_ui ON UniqueIndexTest(f2, f3)");

        executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(100, 10, 'a')");
        executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(200, 20, 'b')");
        executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(300, 30, 'c')");

        try {
            executeUpdate("INSERT INTO UniqueIndexTest(f1, f2, f3) VALUES(400, 20, 'b')");
            fail("insert duplicate key: 20");
        } catch (Exception e) {
            assertErrorCode(e, ErrorCode.DUPLICATE_KEY_1);
        }
    }

    private void init() {
        executeUpdate("DROP TABLE IF EXISTS UniqueIndexTest");
        executeUpdate("create table UniqueIndexTest"
                + "(id bigint primary key, p bigint not null, slot int not null, f int)");
        executeUpdate("create unique index uq_UniqueIndexTest on UniqueIndexTest(p, slot)");
        executeUpdate("insert into UniqueIndexTest values(1,7,1,10)");
    }

    @Test
    public void testCommit() throws Exception {
        init();
        Connection conn1 = getConnection();
        conn1.setAutoCommit(false);
        Connection conn2 = getConnection();

        Statement stmt1 = conn1.createStatement();
        stmt1.executeUpdate("delete from UniqueIndexTest where id=1");
        stmt1.executeUpdate("insert into UniqueIndexTest values(2,7,1,20)");
        sql = "select id,f from UniqueIndexTest where p=7 and slot=1";
        stmt = stmt1;
        assertEquals(2, getIntValue(1, false));
        assertEquals(20, getIntValue(2, true));

        Statement stmt2 = conn2.createStatement();
        stmt = stmt2;
        assertEquals(1, getIntValue(1, false));
        assertEquals(10, getIntValue(2, true));

        conn1.commit();
        stmt = stmt2;
        assertEquals(2, getIntValue(1, false));
        assertEquals(20, getIntValue(2, true));
    }

    @Test
    public void testRollback() throws Exception {
        init();
        Connection conn1 = getConnection();
        conn1.setAutoCommit(false);
        Connection conn2 = getConnection();

        Statement stmt1 = conn1.createStatement();
        stmt1.executeUpdate("delete from UniqueIndexTest where id=1");
        stmt1.executeUpdate("insert into UniqueIndexTest values(2,7,1,20)");
        sql = "select id,f from UniqueIndexTest where p=7 and slot=1";
        stmt = stmt1;
        assertEquals(2, getIntValue(1, false));
        assertEquals(20, getIntValue(2, true));

        Statement stmt2 = conn2.createStatement();
        stmt = stmt2;
        assertEquals(1, getIntValue(1, false));
        assertEquals(10, getIntValue(2, true));

        conn1.rollback();
        stmt = stmt2;
        assertEquals(1, getIntValue(1, false));
        assertEquals(10, getIntValue(2, true));
    }

    @Test
    public void testSavepoint() throws Exception {
        init();
        Connection conn1 = getConnection();
        conn1.setAutoCommit(false);
        Statement stmt1 = conn1.createStatement();
        stmt1.executeUpdate("delete from UniqueIndexTest where id=1");
        stmt1.executeUpdate("savepoint aa");
        stmt1.executeUpdate("insert into UniqueIndexTest values(2,7,1,20)");
        stmt1.executeUpdate("rollback to savepoint aa");
        conn1.commit();
        sql = "select count(*) from UniqueIndexTest where p=7 and slot=1";
        stmt = stmt1;
        assertEquals(0, getIntValue(1, true));
    }

    @Test
    public void testUpdate() throws Exception {
        init();
        Connection conn1 = getConnection();
        conn1.setAutoCommit(false);
        Statement stmt1 = conn1.createStatement();
        stmt1.executeUpdate("UPDATE UniqueIndexTest SET slot=2 WHERE id=1");
        stmt1.executeUpdate("UPDATE UniqueIndexTest SET slot=1 WHERE id=1");
        conn1.commit();
        sql = "select count(*) from UniqueIndexTest where p=7 and slot=1";
        stmt = stmt1;
        assertEquals(1, getIntValue(1, true));
        sql = "select count(*) from UniqueIndexTest where id=1";
        stmt = stmt1;
        assertEquals(1, getIntValue(1, true));
    }

    @Test
    public void testDelete() throws Exception {
        init();
        Connection conn1 = getConnection();
        conn1.setAutoCommit(false);

        Statement stmt1 = conn1.createStatement();
        stmt1.executeUpdate("delete from UniqueIndexTest where id=1");

        try {
            executeUpdate("insert into UniqueIndexTest values(2,7,1,20)");
            fail("insert duplicate key");
        } catch (Exception e) {
            assertErrorCode(e, ErrorCode.DUPLICATE_KEY_1);
        }
        conn1.commit();
        sql = "select count(*) from UniqueIndexTest where p=7 and slot=1";
        stmt = stmt1;
        assertEquals(0, getIntValue(1, true));
    }
}

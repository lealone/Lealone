/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.query;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class PseudoColumnTest extends SqlTestBase {

    public PseudoColumnTest() { // 连接到默认测试数据库
    }

    @Test
    public void run() throws Exception {
        insert();
        select();
    }

    void insert() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS PseudoColumnTest");
        // rownum不能当成字段名
        sql = "create table IF NOT EXISTS PseudoColumnTest(f1 int, f2 int, f3 int, _rowid_ int, rownum int)";

        // 如果_rowid_当成字段名，当insert记录时没指定这个字段的值，那么为null
        // 这时如果select _rowid_ from 就是null
        sql = "create table IF NOT EXISTS PseudoColumnTest(f1 int, f2 int, f3 int, _rowid_ int)";

        // 如果primary key是byte、short、int、long之一，那么select _rowid_ from 就是primary key的值
        sql = "create table IF NOT EXISTS PseudoColumnTest(f1 int primary key, f2 int, f3 int)";
        stmt.executeUpdate(sql);
        stmt.executeUpdate("insert into PseudoColumnTest(f1, f2, f3) values(1,2,3)");
        stmt.executeUpdate("insert into PseudoColumnTest(f1, f2, f3) values(5,2,3)");
        stmt.executeUpdate("insert into PseudoColumnTest(f1, f2, f3) values(3,2,3)");
        stmt.executeUpdate("insert into PseudoColumnTest(f1, f2, f3) values(8,2,3)");

        stmt.executeUpdate("drop table IF EXISTS PseudoColumnTest2");
        stmt.executeUpdate("create table IF NOT EXISTS PseudoColumnTest2(f1 int, f2 int, f3 int)");
        // 手动指定_rowid_为2
        stmt.executeUpdate("insert into PseudoColumnTest2(_rowid_, f1, f2, f3) values(2,8,2,3)");
        // 自动生成的_rowid_从3开始
        stmt.executeUpdate("insert into PseudoColumnTest2(f1, f2, f3) values(8,2,3)");

        sql = "SELECT count(*) FROM PseudoColumnTest2 WHERE _rowid_=3";
        assertEquals(1, getIntValue(1, true));
    }

    void select() throws Exception {
        sql = "select distinct * from PseudoColumnTest where f1 > 3";
        sql = "select distinct f1 from PseudoColumnTest";
        printResultSet();

        sql = "select _rowid_ from PseudoColumnTest";
        printResultSet();

        stmt.executeUpdate("DELETE FROM PseudoColumnTest WHERE f1 = 5");
        stmt.executeUpdate("insert into PseudoColumnTest(f1, f2, f3) values(5,2,3)");
        stmt.executeUpdate("insert into PseudoColumnTest(f1, f2, f3) values(9,2,3)");
        sql = "select _rowid_ from PseudoColumnTest";
        printResultSet();

        stmt.executeUpdate("DELETE FROM PseudoColumnTest WHERE f1 = 9");
        sql = "select _rowid_ from PseudoColumnTest";
        printResultSet();

        stmt.executeUpdate("insert into PseudoColumnTest(f1, f2, f3) values(19,2,3)");
        sql = "select _rowid_,rownum from PseudoColumnTest";
        printResultSet();
    }

}

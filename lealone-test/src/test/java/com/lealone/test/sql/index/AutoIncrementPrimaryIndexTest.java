/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.index;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class AutoIncrementPrimaryIndexTest extends SqlTestBase {

    @Test
    public void run() {
        executeUpdate("drop table IF EXISTS aipkTest");
        executeUpdate("create table IF NOT EXISTS aipkTest" //
                + "(id int auto_increment PRIMARY KEY, name varchar CHECK name>='a1',f2 int default 10)");

        executeUpdate("insert into aipkTest(name) values('a1')");
        executeUpdate("insert into aipkTest(name) values('b1')");
        executeUpdate("insert into aipkTest(name) values('a2')");

        sql = "select * from aipkTest";
        printResultSet();
    }

}

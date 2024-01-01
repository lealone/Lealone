/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.query;

import org.junit.Test;

import com.lealone.db.LealoneDatabase;
import com.lealone.test.sql.SqlTestBase;

public class ShowStatementTest extends SqlTestBase {

    public ShowStatementTest() {
        super(LealoneDatabase.NAME);
    }

    @Test
    public void run() throws Exception {
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS db_client_server RUN MODE client_server");
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS db_replication RUN MODE replication");
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS db_sharding RUN MODE sharding");

        ShowStatementTest t = new ShowStatementTest();
        t.dbName = DEFAULT_DB_NAME;
        t.runTest();

        test();
    }

    @Override
    protected void test() throws Exception {
        p("dbName=" + dbName);
        p("--------------------");
        sql = "show schemas";
        printResultSet();

        sql = "show databases";
        printResultSet();

        sql = "SHOW TABLES";
        printResultSet();

        sql = "select * from information_schema.databases";
        printResultSet();

        sql = "select count(*) from information_schema.databases";
        if (dbName.equals(LealoneDatabase.NAME))
            assertTrue(getIntValue(1, true) >= (4 + 1)); // 至少有5个数据库
        else
            assertEquals(1, getIntValue(1, true));
    }
}

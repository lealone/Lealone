/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.query;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class JoinTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();
        insert();
        test();
    }

    void init() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS JoinTest1");
        executeUpdate("DROP TABLE IF EXISTS JoinTest2");
        executeUpdate("DROP TABLE IF EXISTS JoinTest3");
        executeUpdate("DROP TABLE IF EXISTS JoinTest4");

        executeUpdate("CREATE TABLE IF NOT EXISTS JoinTest1" //
                + "(pk int NOT NULL PRIMARY KEY, id int, name varchar(500), b boolean)");

        executeUpdate("CREATE TABLE IF NOT EXISTS JoinTest2" //
                + "(pk int NOT NULL PRIMARY KEY, id2 int, name2 varchar(500))");

        executeUpdate("CREATE TABLE IF NOT EXISTS JoinTest3" //
                + "(pk int NOT NULL PRIMARY KEY, id3 int, name3 varchar(500))");

        executeUpdate("CREATE TABLE IF NOT EXISTS JoinTest4" //
                + "(pk int NOT NULL PRIMARY KEY, id int, name varchar(500))");
    }

    void insert() throws Exception {
        executeUpdate("insert into JoinTest1(pk, id, name, b) values(1, 10, 'a1', true)");
        executeUpdate("insert into JoinTest1(pk, id, name, b) values(2, 20, 'b1', true)");
        executeUpdate("insert into JoinTest1(pk, id, name, b) values(3, 30, 'a2', false)");
        executeUpdate("insert into JoinTest1(pk, id, name, b) values(4, 40, 'b2', true)");
        executeUpdate("insert into JoinTest1(pk, id, name, b) values(5, 80, 'b2', true)");

        executeUpdate("insert into JoinTest2(pk, id2, name2) values(1, 60, 'a11')");
        executeUpdate("insert into JoinTest2(pk, id2, name2) values(2, 70, 'a11')");
        executeUpdate("insert into JoinTest2(pk, id2, name2) values(3, 80, 'a11')");
        executeUpdate("insert into JoinTest2(pk, id2, name2) values(4, 90, 'a11')");

        executeUpdate("insert into JoinTest3(pk, id3, name3) values(1, 100, 'a11')");
        executeUpdate("insert into JoinTest3(pk, id3, name3) values(2, 200, 'a11')");

        executeUpdate("insert into JoinTest4(pk, id, name) values(1, 10, 'a1')");
        executeUpdate("insert into JoinTest4(pk, id, name) values(2, 10, 'a1')");
        executeUpdate("insert into JoinTest4(pk, id, name) values(3, 20, 'a1')");
        executeUpdate("insert into JoinTest4(pk, id, name) values(4, 30, 'a1')");
    }

    @Override
    protected void test() throws Exception {
        sql = "select rownum, * from JoinTest1 LEFT OUTER JOIN JoinTest2";
        sql = "select rownum, * from JoinTest1 RIGHT OUTER JOIN JoinTest2";
        sql = "select rownum, * from JoinTest1 INNER JOIN JoinTest2";
        sql = "select rownum, * from JoinTest1 JOIN JoinTest2";
        sql = "select rownum, * from JoinTest1 CROSS JOIN JoinTest2";
        sql = "select rownum, * from JoinTest1 NATURAL JOIN JoinTest2";

        sql = "select rownum, * from JoinTest1 LEFT OUTER JOIN JoinTest3 NATURAL JOIN JoinTest2";
        sql = "FROM USER() SELECT * ";

        sql = "SELECT * FROM (JoinTest1)";
        sql = "SELECT * FROM (JoinTest1 LEFT OUTER JOIN (JoinTest2))";

        sql = "SELECT * FROM JoinTest1 LEFT OUTER JOIN JoinTest2 LEFT OUTER JOIN JoinTest3";

        sql = "SELECT t1.id, t1.b FROM JoinTest1 t1 NATURAL JOIN JoinTest4 t2";

        // org.lealone.table.TableFilter.next()
        // 打断点:table.getName().equalsIgnoreCase("JoinTest1") || table.getName().equalsIgnoreCase("JoinTest2")
        sql = "SELECT rownum, * FROM JoinTest1 LEFT OUTER JOIN JoinTest2 ON id>30";
        sql = "SELECT rownum, * FROM JoinTest1 RIGHT OUTER JOIN JoinTest2 ON id2>70";
        sql = "SELECT rownum, * FROM JoinTest1 JOIN JoinTest2 ON id>30";

        sql = "SELECT rownum, * FROM JoinTest1 LEFT OUTER JOIN JoinTest2 JOIN JoinTest3";

        sql = "SELECT rownum, * FROM (JoinTest1) LEFT OUTER JOIN JoinTest2 ON id>30";

        sql = "SELECT rownum, * FROM JoinTest1 LEFT OUTER JOIN (JoinTest2) ON id>30";
        sql = "SELECT rownum, * FROM JoinTest1 JOIN JoinTest2 ON id>30";

        sql = "SELECT rownum, * FROM JoinTest1 LEFT OUTER JOIN JoinTest2 ON id>30 WHERE 1>2";

        sql = "SELECT rownum, * FROM JoinTest1 LEFT OUTER JOIN JoinTest2 ON name2=null";

        sql = "SELECT rownum, * FROM JoinTest1 JOIN JoinTest2 WHERE JoinTest1.pk = 1 and JoinTest2.pk = 2";

        sql = "SELECT rownum, id, id2 FROM JoinTest1 JOIN JoinTest2 WHERE JoinTest1.pk = 1 and JoinTest2.pk = 2";

        sql = "SELECT rownum, * FROM JoinTest1";
        printResultSet();

        sql = "SELECT rownum, t1.id FROM JoinTest1 t1 JOIN JoinTest2 t2";

        sql = "SELECT rownum, t1.id FROM JoinTest1 t1 JOIN JoinTest2 t2 WHERE t1.id = t2.id2";
        printResultSet();

        sql = "SELECT count(*) FROM JoinTest1 t1 JOIN JoinTest2 t2 WHERE t1.id = t2.id2";
        assertEquals(1, getIntValue(1, true));

        sql = "SELECT t1.name FROM JoinTest1 t1 join JoinTest4 t4 ON t1.id = t4.id";
        printResultSet();

        sql = "SELECT count(*) FROM JoinTest1 t1 join JoinTest4 t4 ON t1.id = t4.id";
        assertEquals(4, getIntValue(1, true));
    }
}

package org.yourbase.jdbc;

import static junit.framework.Assert.assertEquals;
import org.junit.Test;

public class SubqueryTest extends TestBase {
    @Test
    public void run() throws Exception {
        init();
        testSelect();
    }

    void init() throws Exception {
        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS SubqueryTest(COLUMN FAMILY cf)");

        stmt.executeUpdate("INSERT INTO SubqueryTest(_rowkey_, f1, f2) VALUES('01', 'a1', 10)");
        stmt.executeUpdate("INSERT INTO SubqueryTest(_rowkey_, f1, f2) VALUES('02', 'a2', 50)");
        stmt.executeUpdate("INSERT INTO SubqueryTest(_rowkey_, f1, f2) VALUES('03', 'a3', 30)");

        stmt.executeUpdate("INSERT INTO SubqueryTest(_rowkey_, f1, f2) VALUES('04', 'a4', 40)");
        stmt.executeUpdate("INSERT INTO SubqueryTest(_rowkey_, f1, f2) VALUES('05', 'a5', 20)");
        stmt.executeUpdate("INSERT INTO SubqueryTest(_rowkey_, f1, f2) VALUES('06', 'a6', 60)");
    }

    void testSelect() throws Exception {
        //TODO select * from 会抛异常
        //见: org.h2.table.HBaseTable.getColumns()
        sql = "SELECT * FROM SubqueryTest WHERE _rowkey_>='01'"
            + " AND f2 >= (SELECT f2 FROM SubqueryTest WHERE _rowkey_='01')";

        //scalar subquery
        sql = "SELECT count(*) FROM SubqueryTest WHERE _rowkey_>='01'"
                + " AND f2 >= (SELECT f2 FROM SubqueryTest WHERE _rowkey_='01')";
        assertEquals(6, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubqueryTest WHERE _rowkey_>='01'"
                + " AND EXISTS(SELECT f2 FROM SubqueryTest WHERE _rowkey_='01' AND f1='a1')";
        assertEquals(6, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubqueryTest WHERE _rowkey_>='01'"
                + " AND f2 IN(SELECT f2 FROM SubqueryTest WHERE _rowkey_>='04')";
        assertEquals(3, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubqueryTest WHERE _rowkey_>='01'"
                + " AND f2 < ALL(SELECT f2 FROM SubqueryTest WHERE _rowkey_>='04')";
        assertEquals(1, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubqueryTest WHERE _rowkey_>='01'"
                + " AND f2 < ANY(SELECT f2 FROM SubqueryTest WHERE _rowkey_>='04')";
        assertEquals(3, getIntValue(1, true));

        sql = "SELECT count(*) FROM SubqueryTest WHERE _rowkey_>='01'"
                + " AND f2 < SOME(SELECT f2 FROM SubqueryTest WHERE _rowkey_>='04')";
        assertEquals(3, getIntValue(1, true));

    }
}

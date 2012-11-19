package org.yourbase.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JDBCTest {
    private static Connection conn;
    private static Statement stmt;
    private String sql;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Properties prop = new Properties();
        prop.setProperty("user", "sa");
        prop.setProperty("password", "");
        String url = "jdbc:h2:hbase:";
        conn = DriverManager.getConnection(url, prop);
        stmt = conn.createStatement();
        createTable();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (stmt != null)
            stmt.close();
        if (conn != null)
            conn.close();
    }

    private static void createTable() throws Exception {
        stmt.executeUpdate("DROP HBASE TABLE IF EXISTS my_hbase_table");

        //CREATE HBASE TABLE语句不用定义字段
        stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS my_hbase_table (" //
                //此OPTIONS对应org.apache.hadoop.hbase.HTableDescriptor的参数选项
                + "OPTIONS(DEFERRED_LOG_FLUSH='false'), "

                //COLUMN FAMILY中的OPTIONS对应org.apache.hadoop.hbase.HColumnDescriptor的参数选项
                + "COLUMN FAMILY cf1 OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true), " //

                + "COLUMN FAMILY cf2 OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true)" //
                + ")");
    }

    @Test
    public void insert() throws Exception {

        //f1没有加列族前缀，默认是cf1，按CREATE HBASE TABLE中的定义顺序，哪个在先默认就是哪个
        //或者在表OPTIONS中指定DEFAULT_COLUMN_FAMILY_NAME参数
        stmt.executeUpdate("INSERT INTO my_hbase_table(_rowkey_, f1, cf1.f2, cf2.f3) VALUES(10, 'a', 'b', 12)");

        stmt.executeUpdate("INSERT INTO my_hbase_table(_rowkey_, f1, cf1.f2, cf2.f3) VALUES(11, 'a1', 'b', 12)");
        stmt.executeUpdate("INSERT INTO my_hbase_table(_rowkey_, f1, cf1.f2, cf2.f3) VALUES(12, 'a2', 'b', 12)");
        stmt.executeUpdate("INSERT INTO my_hbase_table(_rowkey_, f1, cf1.f2, cf2.f3) VALUES(13, 'a2', 'b', 12)");

        //TODO H2数据库会默认把标识符转成大写，这个问题未解决，所以这里表名、列族名用大写
        HTable t = new HTable(HBaseConfiguration.create(), "MY_HBASE_TABLE");
        byte[] cf1 = Bytes.toBytes("CF1");
        byte[] cf2 = Bytes.toBytes("CF2");
        Get get = new Get(Bytes.toBytes("10"));
        Result result = t.get(get);
        Assert.assertEquals("a", toS(result.getValue(cf1, Bytes.toBytes("F1"))));
        Assert.assertEquals("b", toS(result.getValue(cf1, Bytes.toBytes("F2"))));
        Assert.assertEquals("12", toS(result.getValue(cf2, Bytes.toBytes("F3"))));
    }

    @Test
    public void select() throws Exception {
        sql = "from my_hbase_table select f1, f2, cf2.f3";
        executeQuery();

        where();
        orderBy();
        aggregate();
        groupBy();
    }

    @Test
    public void delete() throws Exception {
        stmt.executeUpdate("INSERT INTO my_hbase_table(_rowkey_, f1, cf1.f2, cf2.f3) VALUES(14, 'a2', 'b', 12)");
        sql = "delete from my_hbase_table where _rowkey_=14";
        Assert.assertEquals(1, stmt.executeUpdate(sql));
    }

    private void where() throws Exception {
        sql = "from my_hbase_table select f1, f2, cf2.f3 where f1='a'";
        executeQuery();
    }

    private void orderBy() throws Exception {
        sql = "from my_hbase_table select f1, f2, cf2.f3 order by f1 desc";
        executeQuery();
    }

    private void aggregate() throws Exception {
        sql = "select count(*), max(f1), min(f1) from my_hbase_table";
        executeQuery();
    }

    private void groupBy() throws Exception {
        sql = "select f1, count(f1) from my_hbase_table group by f1";
        sql = "select f1, count(f1) from my_hbase_table group by f1 having f1>'a1'";
        executeQuery();
    }

    private void executeQuery() throws Exception {
        ResultSet rs = stmt.executeQuery(sql);

        int n = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= n; i++) {
                System.out.print(rs.getString(i) + " ");
            }
            System.out.println();
        }
        rs.close();
        System.out.println();
    }

    public static String toS(byte[] v) {
        return Bytes.toString(v);
    }
}

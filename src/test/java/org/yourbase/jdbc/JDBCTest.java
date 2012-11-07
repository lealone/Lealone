package org.yourbase.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
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

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Properties prop = new Properties();
		prop.setProperty("user", "sa");
		prop.setProperty("password", "");
		String url = "jdbc:h2:hbase:";
		conn = DriverManager.getConnection(url, prop);
		stmt = conn.createStatement();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		if (stmt != null)
			stmt.close();
		if (conn != null)
			conn.close();
	}

	@Test
	public void insert() throws Exception {
		stmt.executeUpdate("DROP HBASE TABLE IF EXISTS my_hbase_table");

		//CREATE HBASE TABLE语句不用定义字段
		stmt.executeUpdate("CREATE HBASE TABLE IF NOT EXISTS my_hbase_table (" //
				//此OPTIONS对应org.apache.hadoop.hbase.HTableDescriptor的参数选项
				+ "OPTIONS(DEFERRED_LOG_FLUSH='false'), "

				//COLUMN FAMILY中的OPTIONS对应org.apache.hadoop.hbase.HColumnDescriptor的参数选项
				+ "COLUMN FAMILY cf1 OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true), " //

				+ "COLUMN FAMILY cf2 OPTIONS(MIN_VERSIONS=2, KEEP_DELETED_CELLS=true)" //
				+ ")");

		//f1没有加列族前缀，默认是cf1，按CREATE HBASE TABLE中的定义顺序，哪个在些默认就是哪个
		//或者在表OPTIONS中指定DEFAULT_COLUMN_FAMILY_NAME参数
		stmt.executeUpdate("INSERT INTO my_hbase_table(_rowkey_, f1, cf1.f2, cf2.f3) VALUES(10, 'a', 'b', 12)");

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

	public static String toS(byte[] v) {
		return Bytes.toString(v);
	}
}

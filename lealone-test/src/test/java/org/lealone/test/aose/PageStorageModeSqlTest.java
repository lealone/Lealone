/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import org.junit.Test;
import org.lealone.storage.aose.AOStorageEngine;
import org.lealone.storage.aose.btree.page.PageStorageMode;
import org.lealone.test.sql.SqlTestBase;

public class PageStorageModeSqlTest extends SqlTestBase {

    private int rowCount = 6000;
    private int columnCount = 20;

    public PageStorageModeSqlTest() {
        super("PageStorageModeSqlTest");
        setEmbedded(true);
        addConnectionParameter("PAGE_SIZE", 2 * 1024 * 1024);
        // addConnectionParameter("COMPRESS", "true");

        printURL();
    }

    @Test
    public void run() throws Exception {
        testRowStorage();
        testColumnStorage();
    }

    private void testRowStorage() {
        testCRUD("testRowStorage", PageStorageMode.ROW_STORAGE);
    }

    private void testColumnStorage() {
        testCRUD("testColumnStorage", PageStorageMode.COLUMN_STORAGE);
    }

    private void testCRUD(String tableName, PageStorageMode pageStorageMode) {
        executeUpdate("SET OPTIMIZE_REUSE_RESULTS 0");
        long t0 = System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        createTestTable(tableName, pageStorageMode.name());
        putData(tableName);
        long t2 = System.currentTimeMillis();
        // System.out.println(pageStorageMode + " create table time: " + (t2 - t1) + " ms");

        // t1 = System.currentTimeMillis();
        // sql = "select f2 from " + tableName + " where pk = 2";
        // printResultSet();
        // t2 = System.currentTimeMillis();
        // System.out.println(pageStorageMode + " select time: " + (t2 - t1) + " ms");
        //
        // t1 = System.currentTimeMillis();
        // sql = "select f2 from " + tableName + " where pk = 2000";
        // printResultSet();
        // t2 = System.currentTimeMillis();
        // System.out.println(pageStorageMode + " select time: " + (t2 - t1) + " ms");

        t1 = System.currentTimeMillis();
        sql = "select sum(pk),count(f6) from " + tableName + " where pk >= 2000";
        int sum = 0;
        int count = 0;
        try {
            sum = getIntValue(1);
            count = getIntValue(2, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        t2 = System.currentTimeMillis();
        System.out.println(pageStorageMode + " agg time: " + (t2 - t1) + " ms" + ", count: " + count + ", sum: " + sum);

        System.out.println(pageStorageMode + " total time: " + (t2 - t0) + " ms");
    }

    private void createTestTable(String tableName, String pageStorageMode) {
        // executeUpdate("drop table IF EXISTS " + tableName);
        StringBuilder sql = new StringBuilder("create table IF NOT EXISTS ").append(tableName)
                .append("(pk int primary key");
        for (int col = 1; col <= columnCount; col++) {
            sql.append(", f").append(col).append(" varchar");
        }
        sql.append(") Engine ").append(AOStorageEngine.NAME).append(" PARAMETERS(pageStorageMode='")
                .append(pageStorageMode).append("')");
        executeUpdate(sql.toString());
    }

    private void putData(String tableName) {
        sql = "select count(*) from " + tableName;
        int count = 0;
        try {
            count = getIntValue(1, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (count <= 0) {
            // Random random = new Random();
            for (int row = 1; row <= rowCount; row++) {
                StringBuilder sql = new StringBuilder("insert into ").append(tableName).append(" values(").append(row);
                for (int col = 1; col <= columnCount; col++) {
                    sql.append(", 'value-row" + row + "-col" + col + "'");
                    // columns[col] = ValueString.get("a string");
                    // int randomIndex = random.nextInt(columnCount);
                    // columns[col] = ValueString.get("value-" + randomIndex);
                    // if (col % 2 == 0) {
                    // columns[col] = ValueString.get("a string");
                    // } else {
                    // columns[col] = ValueString.get("value-row" + row + "-col" + (col + 1));
                    // }
                }
                sql.append(")");
                // System.out.println(Arrays.asList(columns));
                executeUpdate(sql.toString());
            }
            executeUpdate("checkpoint");
            // map.remove();
        }
    }
}

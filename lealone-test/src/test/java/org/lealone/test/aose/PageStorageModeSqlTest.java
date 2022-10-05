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

    private int rowCount = 1000;
    private int columnCount = 10;

    public PageStorageModeSqlTest() {
        super("PageStorageModeSqlTest");
        setEmbedded(true);
        addConnectionParameter("PAGE_SIZE", 2 * 1024 * 1024);
        // addConnectionParameter("COMPRESS", "true");
        // printURL();
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
        createTestTable(tableName, pageStorageMode.name());
        putData(tableName);

        sql = "select count(*) from " + tableName + " where pk > 500";
        int count = 0;
        try {
            count = getIntValue(1, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertEquals(500, count);
    }

    private void createTestTable(String tableName, String pageStorageMode) {
        executeUpdate("drop table IF EXISTS " + tableName);
        StringBuilder sql = new StringBuilder("create table IF NOT EXISTS ").append(tableName)
                .append("(pk int primary key");
        for (int col = 1; col <= columnCount; col++) {
            sql.append(", f").append(col).append(" varchar");
        }
        sql.append(") Engine ").append(AOStorageEngine.NAME).append(" PARAMETERS(pageStorageMode='")
                .append(pageStorageMode).append("', pageSplitSize='512k')");
        executeUpdate(sql.toString());
    }

    private void putData(String tableName) {
        for (int row = 1; row <= rowCount; row++) {
            StringBuilder sql = new StringBuilder("insert into ").append(tableName).append(" values(")
                    .append(row);
            for (int col = 1; col <= columnCount; col++) {
                sql.append(", 'value-row" + row + "-col" + col + "'");
            }
            sql.append(")");
            executeUpdate(sql.toString());
        }
        executeUpdate("checkpoint");
    }
}

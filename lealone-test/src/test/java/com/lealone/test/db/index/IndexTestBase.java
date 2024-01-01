/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db.index;

import java.util.ArrayList;

import com.lealone.db.index.Index;
import com.lealone.db.table.Table;
import com.lealone.test.db.DbObjectTestBase;

public abstract class IndexTestBase extends DbObjectTestBase {

    protected void assertFound(String tableName, String indexName) {
        Table table = schema.findTableOrView(session, tableName);
        assertNotNull(table);
        Index index = schema.findIndex(session, indexName);
        assertNotNull(index);
        ArrayList<Index> indexes = table.getIndexes();
        assertTrue(indexes.contains(index));
    }

    protected void assertNotFound(String tableName, String indexName) {
        Table table = schema.findTableOrView(session, tableName);
        assertNotNull(table);
        Index index = schema.findIndex(session, indexName);
        assertNull(index);
        ArrayList<Index> indexes = table.getIndexes();
        assertFalse(indexes.contains(index));
    }

    protected Index getIndex(String indexName) {
        return schema.getIndex(session, indexName);
    }
}

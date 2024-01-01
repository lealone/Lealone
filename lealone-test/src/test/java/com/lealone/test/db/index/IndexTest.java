/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db.index;

import org.junit.Test;

import com.lealone.db.Constants;
import com.lealone.db.index.Index;
import com.lealone.db.table.Table;
import com.lealone.test.db.DbObjectTestBase;

public class IndexTest extends DbObjectTestBase {

    @Test
    public void run() {
        create();
        alter();
        drop();
    }

    void create() {
        executeUpdate("DROP TABLE IF EXISTS CreateIndexTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS CreateIndexTest (f1 int NOT NULL, f2 int, f3 int)");

        executeUpdate(
                "CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS TestTempTable1 (f1 int NOT NULL, f2 int, f3 int)");
        executeUpdate("CREATE INDEX IF NOT EXISTS TestTempTableIndex1 ON TestTempTable1(f1)");

        Table table = schema.findTableOrView(session, "TestTempTable1");
        assertNotNull(table);
        Index index = schema.findIndex(session, "TestTempTableIndex1");
        assertNotNull(index);

        executeUpdate("DROP INDEX IF EXISTS TestTempTableIndex1");
        executeUpdate("DROP TABLE IF  EXISTS TestTempTable1");

        table = schema.findTableOrView(session, "TestTempTable1");
        assertNull(table);
        index = schema.findIndex(session, "TestTempTableIndex1");
        assertNull(index);

        table = schema.findTableOrView(session, "CreateIndexTest");
        assertNotNull(table);

        executeUpdate("CREATE PRIMARY KEY HASH ON CreateIndexTest(f1)");
        index = table.findPrimaryKey();
        assertNotNull(index);
        String indexName = index.getName();
        assertTrue(indexName.startsWith(Constants.PREFIX_PRIMARY_KEY));

        executeUpdate("DROP INDEX IF EXISTS " + indexName);
        index = schema.findIndex(session, indexName);
        assertNull(index);

        executeUpdate("CREATE PRIMARY KEY HASH IF NOT EXISTS idx0 ON CreateIndexTest(f1)");
        index = schema.findIndex(session, "idx0");
        assertNotNull(index);

        executeUpdate("CREATE UNIQUE HASH INDEX IF NOT EXISTS idx1 ON CreateIndexTest(f2)");
        index = schema.findIndex(session, "idx1");
        assertNotNull(index);

        executeUpdate("CREATE INDEX IF NOT EXISTS idx2 ON CreateIndexTest(f3)");
        index = schema.findIndex(session, "idx2");
        assertNotNull(index);
    }

    void alter() {
        // executeUpdate("CREATE SCHEMA s1 AUTHORIZATION root");
        // executeUpdate("ALTER INDEX idx2 RENAME TO DbObjectTest.s1.idx22");

        // executeUpdate("ALTER INDEX idx2 RENAME TO DbObjectTest.PUBLIC.idx22");
        executeUpdate("ALTER INDEX idx2 RENAME TO idx22");

        Index index = schema.findIndex(session, "idx22");
        assertNotNull(index);
        index = schema.findIndex(session, "idx2");
        assertNull(index);
    }

    void drop() {
        executeUpdate("DROP INDEX IF EXISTS idx22");
        Index index = schema.findIndex(session, "idx22");
        assertNull(index);
    }
}

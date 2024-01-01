/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aose;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class StorageSqlTest extends SqlTestBase {

    public StorageSqlTest() {
        super("StorageSqlTest");
        setEmbedded(true);
    }

    @Test
    public void run() {
        executeUpdate("CREATE TABLE IF NOT EXISTS StorageSqlTest(f1 int, f2 int)" //
                + " ENGINE " + DEFAULT_STORAGE_ENGINE_NAME //
                + " PARAMETERS(map_type='BTreeMap')");
        executeUpdate("INSERT INTO StorageSqlTest(f1, f2) VALUES(1, 10)");
        executeUpdate("INSERT INTO StorageSqlTest(f1, f2) VALUES(2, 20)");

        sql = "SELECT * FROM StorageSqlTest";
        printResultSet();
    }
}

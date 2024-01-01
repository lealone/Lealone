/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.ddl;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class TruncateTableTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS TruncateTableTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS TruncateTableTest (f1 int,f2 int)");
        executeUpdate("INSERT INTO TruncateTableTest VALUES(1,3)");
        executeUpdate("INSERT INTO TruncateTableTest VALUES(2,1)");
        executeUpdate("INSERT INTO TruncateTableTest VALUES(3,2)");
        executeUpdate("CREATE INDEX IF NOT EXISTS TruncateTableTest_idx2 ON TruncateTableTest(f2)");
        executeUpdate("TRUNCATE TABLE TruncateTableTest");
    }

}

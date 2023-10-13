/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db;

import org.junit.Test;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.SearchRow;

public class DatabaseTest extends DbObjectTestBase {

    public DatabaseTest() {
        super(LealoneDatabase.NAME);
    }

    private void asserts(String dbName) {
        int id;
        Database db;

        db = findDatabase(dbName);
        assertNotNull(db);
        id = db.getId();
        assertTrue(id > 0);
        SearchRow row = LealoneDatabase.getInstance().findMeta(session, id);
        if (db.isPersistent()) // 只有非内存数据库才会在meta表中保存一条记录用来代表它
            assertNotNull(row);
        else
            assertNull(row);
    }

    @Test
    public void run() {
        executeUpdate("CREATE DATABASE IF NOT EXISTS CreateDatabaseTest1");
        asserts("CreateDatabaseTest1");

        executeUpdate("CREATE DATABASE IF NOT EXISTS CreateDatabaseTest2"
                + " PARAMETERS(OPTIMIZE_DISTINCT=true, PERSISTENT=false)");
        asserts("CreateDatabaseTest2");

        executeUpdate("CREATE DATABASE IF NOT EXISTS CreateDatabaseTest3 PARAMETERS()");
        asserts("CreateDatabaseTest3");

        try {
            executeUpdate("CREATE DATABASE CreateDatabaseTest1");
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof DbException);
            assertEquals(ErrorCode.DATABASE_ALREADY_EXISTS_1, ((DbException) e).getErrorCode());
        }

        try {
            executeUpdate("CREATE DATABASE " + LealoneDatabase.NAME);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof DbException);
            assertEquals(ErrorCode.DATABASE_ALREADY_EXISTS_1, ((DbException) e).getErrorCode());
        }

        try {
            executeUpdate("CREATE DATABASE IF NOT EXISTS CreateDatabaseTestXxx PARAMETERS(xxxx=false)");
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unrecognized parameters"));
        }
    }
}

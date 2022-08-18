/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.index;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class DelegateIndexTest extends SqlTestBase {
    @Test
    public void run() {
        executeUpdate("DROP TABLE IF EXISTS DelegateIndexTest");
        executeUpdate(
                "CREATE TABLE IF NOT EXISTS DelegateIndexTest(date_time TIMESTAMP primary key, intcol INT)");

        executeUpdate(
                "INSERT INTO DelegateIndexTest(date_time, intcol) VALUES('1970-01-01 00:00:01.0', 12)");

        sql = "select * from DelegateIndexTest where date_time='1970-01-01 00:00:01.0'";
        printResultSet();
    }
}

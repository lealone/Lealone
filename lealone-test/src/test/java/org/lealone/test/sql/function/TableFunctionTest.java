/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.function;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class TableFunctionTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        sql = "SELECT * FROM TABLE(ID INT=(1, 2), NAME VARCHAR=('Hello', 'World'))";
        assertEquals(2, printResultSet());
        sql = "SELECT * FROM TABLE_DISTINCT(ID INT=(1, 2, 2), NAME VARCHAR=('Hello', 'World', 'World'))";
        assertEquals(2, printResultSet());

        sql = "VALUES(1,2),(10,20)";
        assertEquals(2, printResultSet());
        sql = "SELECT * FROM VALUES(1,2),(10,20)";
        assertEquals(2, printResultSet());
    }
}

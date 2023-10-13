/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db.table;

import org.junit.Test;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.Result;
import org.lealone.test.db.DbObjectTestBase;

public class FunctionTableTest extends DbObjectTestBase {
    @Test
    public void run() {
        Result rs = executeQuery("VALUES(1,2),(10,20)");
        assertTrue(rs.next());
        assertEquals(1, getInt(rs, 1));
        assertEquals(2, getInt(rs, 2));
        assertTrue(rs.next());
        assertEquals(10, getInt(rs, 1));
        assertEquals(20, getInt(rs, 2));
        rs.close();

        rs = executeQuery("select count(*) from VALUES(1,2),(10,20)");
        assertTrue(rs.next());
        assertEquals(2, getInt(rs, 1));
        rs.close();

        try {
            executeQuery("select * from UPPER('abc')");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1);
        }

        rs = executeQuery("select * from TABLE(ID INT=(1, 2), NAME VARCHAR=(UPPER('Hello'), 'World'))");
        assertTrue(rs.next());
        assertEquals(1, getInt(rs, 1));
        assertEquals("HELLO", getString(rs, 2));
        assertTrue(rs.next());
        assertEquals(2, getInt(rs, 1));
        assertEquals("World", getString(rs, 2));
        rs.close();

        // TABLE_DISTINCT内部使用LocalResult.distinctRows来存放唯一记录，顺序不一定是按sql字符串中出现的顺序来
        rs = executeQuery(
                "select * from TABLE_DISTINCT(ID INT=(1, 2), NAME VARCHAR=(UPPER('Hello'), 'World'))");
        assertTrue(rs.next());
        p(getInt(rs, 1) + ", " + getString(rs, 2));
        assertTrue(rs.next());
        p(getInt(rs, 1) + ", " + getString(rs, 2));
        rs.close();

        rs = executeQuery(
                "select count(*) from TABLE_DISTINCT(ID INT=(1, 1), NAME VARCHAR=(UPPER('Hello'), 'Hello'))");
        assertTrue(rs.next());
        assertEquals(2, getInt(rs, 1));
        rs.close();
    }
}

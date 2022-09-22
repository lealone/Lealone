/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db.table;

import org.junit.Test;
import org.lealone.db.result.Result;
import org.lealone.db.table.RangeTable;
import org.lealone.test.db.DbObjectTestBase;

public class RangeTableTest extends DbObjectTestBase {
    @Test
    public void run() {
        String name = RangeTable.NAME;
        Result rs = executeQuery("select count(*) from " + name + "(1,10)");
        assertTrue(rs.next());
        assertEquals(10, getInt(rs, 1));
        rs.close();

        rs = executeQuery("select count(*) from " + name + "(1,10,2)");
        assertTrue(rs.next());
        assertEquals(10, getInt(rs, 1));
        rs.close();

        rs = executeQuery("select * from " + name + "(1,10,2)");
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertEquals(3, getInt(rs, 1));
        rs.close();
    }
}

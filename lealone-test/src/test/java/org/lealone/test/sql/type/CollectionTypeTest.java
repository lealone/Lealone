/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.type;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class CollectionTypeTest extends SqlTestBase {
    @Test
    public void run() {
        executeUpdate("drop table if exists CollectionTypeTest");
        executeUpdate("create table if not exists CollectionTypeTest "
                + "(f1 list<int>,f2 set<varchar>,f3 map<int, varchar>)");

        try {
            executeUpdate("insert into CollectionTypeTest(f1) values((1,'a',3.1))");
            fail();
        } catch (Exception e) {
        }
        executeUpdate("insert into CollectionTypeTest(f2) values((1,'a',3.1))");
        try {
            executeUpdate("insert into CollectionTypeTest(f3) values((1:1,'a':2))");
            fail();
        } catch (Exception e) {
        }
        try {
            executeUpdate("update CollectionTypeTest set f1=(1,'a',3.1)");
            fail();
        } catch (Exception e) {
        }
        try {
            executeUpdate("update CollectionTypeTest set f3=(1:1,'q':2)");
            fail();
        } catch (Exception e) {
        }

        executeUpdate("update CollectionTypeTest set f3=(1:1,'2':2)");
        sql = "DELETE FROM CollectionTypeTest";
        assertEquals(1, executeUpdate(sql));
    }
}

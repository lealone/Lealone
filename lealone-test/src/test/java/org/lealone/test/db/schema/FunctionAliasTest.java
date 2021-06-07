/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db.schema;

import org.junit.Test;
import org.lealone.db.result.Result;
import org.lealone.test.db.DbObjectTestBase;

//Ubuntu环境下运行这个测试，如果出现错误找不到javac，需要在~/.profile文件中把$JAVA_HOME/bin加到$PATH中
public class FunctionAliasTest extends DbObjectTestBase {

    @Test
    public void run() {
        create();
        drop();
    }

    void create() {
        executeUpdate("CREATE ALIAS IF NOT EXISTS my_sqrt DETERMINISTIC FOR \"java.lang.Math.sqrt\"");

        // 用$$与用单引号有一样的效果
        executeUpdate("CREATE ALIAS IF NOT EXISTS my_reverse AS "
                + "$$ String reverse(String s) { return new StringBuilder(s).reverse().toString(); } $$");

        assertNotNull(schema.findFunction(session, "my_sqrt"));
        assertNotNull(schema.findFunction(session, "my_reverse"));

        sql = "select my_sqrt(4.0), my_reverse('abc')";
        Result result = executeQuery(sql);
        assertTrue(result.next());
        assertEquals("2.0", getString(result, 1));
        assertEquals("cba", getString(result, 2));
    }

    void drop() {
        executeUpdate("DROP ALIAS IF EXISTS my_sqrt");
        executeUpdate("DROP ALIAS IF EXISTS my_reverse");

        assertNull(schema.findFunction(session, "my_sqrt"));
        assertNull(schema.findFunction(session, "my_reverse"));
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.dml;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class ScriptTest extends SqlTestBase {
    @Test
    public void run() {
        sql = "SCRIPT NODATA TO 'my_script_test.sql'"; // 生成各种Create SQL，此命令返回结果集，所以要用executeQuery
        printResultSet();
    }
}

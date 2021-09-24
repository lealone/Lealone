/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.expression;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class VariableTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        stmt.executeUpdate("SET @topVariableName=3");
        sql = "select @topVariableName";
        assertEquals(3, getIntValue(1, true));

        sql = "select @topVariableName:=2"; // 这样就变成SET函数了

        sql = "select @nullVariableName"; // 不存在的变量名，此时值为null
        assertEquals(null, getStringValue(1, true));
    }
}

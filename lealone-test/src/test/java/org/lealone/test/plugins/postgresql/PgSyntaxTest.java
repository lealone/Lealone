/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.plugins.postgresql;

import org.junit.Test;

public class PgSyntaxTest extends PgTestBase {
    @Test
    public void run() throws Exception {
        testFormatType();
    }

    void testFormatType() throws Exception {
        executeQuery("select format_type(4, 0)");
        assertEquals("PG_TYPE_INT4", getStringValue(1));
    }
}

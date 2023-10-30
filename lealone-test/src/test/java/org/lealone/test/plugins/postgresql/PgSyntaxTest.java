/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.plugins.postgresql;

import org.junit.Test;

public class PgSyntaxTest extends PgTestBase {
    @Test
    public void testFormatType() throws Exception {
        executeQuery("select format_type(4, 0)");
        assertEquals("PG_TYPE_INT4", getStringValue(1));
    }

    @Test
    public void testShowStatement() throws Exception {
        executeQuery("SHOW SEARCH_PATH");
        assertEquals("public,pg_catalog", getStringValue(1));
        executeQuery("SHOW ALL");
    }

    @Test
    public void testTransactionStatement() throws Exception {
        executeUpdate("BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE DEFERRABLE");
        executeUpdate("END");
        executeUpdate("START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE DEFERRABLE");
        executeUpdate("COMMIT");
    }
}

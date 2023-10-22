/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.plugins.mysql;

import org.junit.Test;

public class MySQLSyntaxTest extends MySQLTestBase {
    @Test
    public void run() throws Exception {
        testTransactionIsolation();
        testBinaryColumn();
    }

    void testTransactionIsolation() throws Exception {
        executeUpdate("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
        executeQuery("select @@session.tx_isolation");
        assertEquals("READ-COMMITTED", getStringValue(1));
    }

    void testBinaryColumn() throws Exception {
        executeUpdate("drop table if exists varcharbinary");
        executeUpdate("CREATE TABLE varcharbinary (pk varchar(100) BINARY NOT NULL PRIMARY KEY, " + //
                "f1 varchar(100), f2 varchar(100), f3 int, f4 BINARY(1000))");
    }
}

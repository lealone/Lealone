/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.service;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class DropServiceTest extends SqlTestBase {
    @Test
    public void dropService() {
        executeUpdate("drop service if exists test_service");
    }
}
/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.service;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class CreateServiceTest extends SqlTestBase {
    @Test
    public void createService() {
        executeUpdate("drop service if exists generated_service");
        String sql = "create service if not exists generated_service (" //
                + " test(name varchar) int)" //
                + " implement by '" + ExecuteServiceTest.class.getPackage().getName()
                + ".generated.GeneratedService'" //
                + " code path './src/test/java'";
        executeUpdate(sql);
    }
}

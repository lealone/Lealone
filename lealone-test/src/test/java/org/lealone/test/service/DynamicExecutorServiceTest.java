/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.service;

import org.junit.Test;
import org.lealone.test.orm.generated.User;
import org.lealone.test.service.impl.DynamicExecutorServiceImpl;
import org.lealone.test.sql.SqlTestBase;

public class DynamicExecutorServiceTest extends SqlTestBase {
    @Test
    public void testService() throws Exception {
        executeUpdate("drop service if exists dynamic_executor_service");
        sql = "create service if not exists dynamic_executor_service (" //
                + " add(user user) long," // 第一个user是参数名，第二个user是参数类型
                + " delete(name varchar) int," //
                + " find(name varchar) user)" //
                + " implement by '" + DynamicExecutorServiceImpl.class.getName() + "'";
        executeUpdate(sql);

        sql = " EXECUTE SERVICE dynamic_executor_service delete('dynamic')";
        executeQuery(sql);

        User user = new User().name.set("dynamic").phone.set(123);
        sql = "EXECUTE SERVICE dynamic_executor_service add('" + user.encode() + "')";
        executeQuery(sql);

        sql = " EXECUTE SERVICE dynamic_executor_service find('dynamic')";
        executeQuery(sql);
        String str = getStringValue(1, true);
        assertEquals("dynamic", User.decode(str).name.get());
    }
}

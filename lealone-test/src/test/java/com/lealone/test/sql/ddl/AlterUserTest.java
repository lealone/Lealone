/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.ddl;

import java.sql.Connection;

import org.junit.Test;

import com.lealone.db.LealoneDatabase;
import com.lealone.test.sql.SqlTestBase;

//之前存在bug: 内部实现用用户名和密码组成hash，重命名用户后不能通过原来的密码登录
//这个测试用例就是用来测试这个bug
public class AlterUserTest extends SqlTestBase {

    public AlterUserTest() {
        super(LealoneDatabase.NAME);
    }

    @Test
    public void run() throws Exception {
        stmt.executeUpdate("DROP USER IF EXISTS test1");
        stmt.executeUpdate("DROP USER IF EXISTS test2");
        stmt.executeUpdate("CREATE USER IF NOT EXISTS test1 PASSWORD 'test' ADMIN");
        try (Connection conn = getConnection("test1", "test")) {
        } catch (Exception e) {
            fail();
        }
        stmt.executeUpdate("ALTER USER test1 RENAME TO test2");
        try (Connection conn = getConnection("test2", "test")) {
        } catch (Exception e) {
            fail();
        }
    }

}

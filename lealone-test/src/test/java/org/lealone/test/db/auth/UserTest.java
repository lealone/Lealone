/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.db.auth;

import org.junit.Test;
import org.lealone.api.ErrorCode;
import org.lealone.db.auth.User;
import org.lealone.db.result.SearchRow;
import org.lealone.test.db.DbObjectTestBase;

public class UserTest extends DbObjectTestBase {

    private void asserts(String userName) {
        User user = findUser(userName);
        int id = user.getId();
        assertTrue(id > 0);
        assertEquals(userName, user.getName());

        assertTrue(!user.isTemporary());
        SearchRow row = findMeta(id);
        assertNotNull(row);
        assertEquals(id, row.getValue(0).getInt());
    }

    @Test
    public void run() {
        create();
        alter();
        drop();
    }

    void create() {
        // CREATE USER支持三种指定密码的语法

        // 第1种用PASSWORD
        // COMMENT IS用来指定注释，可以简写成COMMENT
        executeUpdate("CREATE USER IF NOT EXISTS sa1 COMMENT IS 'a amdin user' PASSWORD 'abc' ADMIN");
        asserts("sa1");

        // 第2种用SALT和HASH
        // SALT和HASH必须是16进制的，X不加也是可以的
        // X'...'必须是偶数个
        executeUpdate("CREATE USER IF NOT EXISTS sa2 COMMENT 'a amdin user with salt' SALT X'123456' HASH X'78' ADMIN");
        asserts("sa2");

        // 第3种用IDENTIFIED BY
        executeUpdate("CREATE USER IF NOT EXISTS sa3 IDENTIFIED BY abc"); // 密码不加引号
        asserts("sa3");

        // 下面是错误的
        try {
            executeUpdate("CREATE USER IF NOT EXISTS sa3 PASWORD 'abc'"); // PASWORD少了一个S
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.SYNTAX_ERROR_2);
        }

        // 虽然sa1存在了，但是使用了IF NOT EXISTS
        executeUpdate("CREATE USER IF NOT EXISTS sa1 PASSWORD 'abc'");
        asserts("sa1");

        try {
            // 没有使用IF NOT EXISTS时就抛错
            executeUpdate("CREATE USER sa1 PASSWORD 'abc'");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.USER_ALREADY_EXISTS_1);
        }

        executeUpdate("CREATE ROLE IF NOT EXISTS role1");
        try {
            // 用户名不能和前面的角色名一样
            executeUpdate("CREATE USER IF NOT EXISTS role1 PASSWORD 'abc'");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.ROLE_ALREADY_EXISTS_1);
        } finally {
            executeUpdate("DROP ROLE IF EXISTS role1");
        }
    }

    void alter() {
        executeUpdate("ALTER USER sa1 SET PASSWORD 'abc123'");
        asserts("sa1");

        executeUpdate("ALTER USER sa2 SET SALT X'12345678' HASH X'7890'");
        asserts("sa2");

        executeUpdate("ALTER USER sa3 RENAME TO sa33");
        asserts("sa33");

        try {
            executeUpdate("ALTER USER sa33 RENAME TO sa2");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.USER_ALREADY_EXISTS_1);
        }

        executeUpdate("ALTER USER sa33 ADMIN true");
        asserts("sa33");
        User user = findUser("sa33");
        assertTrue(user.isAdmin());

        // 去掉用户的Admin权限前，前需要删除它拥有的所有Schema，否则不允许
        executeUpdate("CREATE SCHEMA IF NOT EXISTS UserTest_schema1 AUTHORIZATION sa33");
        try {
            executeUpdate("ALTER USER sa33 ADMIN false");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.CANNOT_DROP_2);
        } finally {
            executeUpdate("DROP SCHEMA IF EXISTS UserTest_schema1");
        }

        executeUpdate("ALTER USER sa33 ADMIN false");
        asserts("sa33");
        user = findUser("sa33");
        assertFalse(user.isAdmin());

    }

    void drop() {
        User user = findUser("sa33");
        assertNotNull(user);
        int id = user.getId();
        executeUpdate("DROP USER sa33");
        user = findUser("sa33");
        assertNull(user);
        SearchRow row = findMeta(id);
        assertNull(row);
        try {
            executeUpdate("DROP USER UserTest_Not_Exists_User");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.USER_NOT_FOUND_1);
        }

        // 删除用户前需要删除它拥有的所有Schema，否则不允许删
        executeUpdate("CREATE SCHEMA IF NOT EXISTS UserTest_schema1 AUTHORIZATION sa2");
        try {
            executeUpdate("DROP USER sa2");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.CANNOT_DROP_2);
        } finally {
            executeUpdate("DROP SCHEMA IF EXISTS UserTest_schema1");
        }

        user = findUser("sa2");
        assertNotNull(user);

        executeUpdate("DROP USER sa2");
        user = findUser("sa2");
        assertNull(user);
    }

}

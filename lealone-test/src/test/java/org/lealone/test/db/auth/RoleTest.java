/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db.auth;

import org.junit.Test;
import org.lealone.db.Constants;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.Role;
import org.lealone.db.result.SearchRow;
import org.lealone.test.db.DbObjectTestBase;

public class RoleTest extends DbObjectTestBase {

    private void asserts(String roleName) {
        Role role = findRole(roleName);
        int id = role.getId();
        assertTrue(id > 0);
        assertEquals(roleName, role.getName());

        assertTrue(!role.isTemporary());
        SearchRow row = findMeta(id);
        assertNotNull(row);
        assertEquals(id, row.getValue(0).getInt());
    }

    @Test
    public void run() {
        create();
        drop();
    }

    void create() {
        executeUpdate("CREATE ROLE IF NOT EXISTS r1");
        asserts("r1");

        // 虽然r1存在了，但是使用了IF NOT EXISTS
        executeUpdate("CREATE ROLE IF NOT EXISTS r1");
        asserts("r1");

        try {
            // 没有使用IF NOT EXISTS时就抛错
            executeUpdate("CREATE ROLE r1");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.ROLE_ALREADY_EXISTS_1);
        }

        executeUpdate("CREATE USER IF NOT EXISTS RoleTest_u1 PASSWORD 'abc'");
        try {
            // 角色名不能和前面的用户名一样
            executeUpdate("CREATE ROLE IF NOT EXISTS RoleTest_u1");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.USER_ALREADY_EXISTS_1);
        } finally {
            executeUpdate("DROP USER IF EXISTS RoleTest_u1");
        }
    }

    void drop() {
        Role role = findRole("r1");
        int id = role.getId();
        assertNotNull(role);
        executeUpdate("DROP ROLE r1");
        role = findRole("r1");
        assertNull(role);
        SearchRow row = findMeta(id);
        assertNull(row);
        try {
            executeUpdate("DROP ROLE r1");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.ROLE_NOT_FOUND_1);
        }

        try {
            executeUpdate("DROP ROLE " + Constants.PUBLIC_ROLE_NAME);
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.ROLE_CAN_NOT_BE_DROPPED_1);
        }
    }
}

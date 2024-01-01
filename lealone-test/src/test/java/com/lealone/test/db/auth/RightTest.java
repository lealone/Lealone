/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db.auth;

import org.junit.Test;

import com.lealone.db.Constants;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.auth.Role;
import com.lealone.db.auth.User;
import com.lealone.db.schema.Schema;
import com.lealone.db.table.Table;
import com.lealone.test.db.DbObjectTestBase;

public class RightTest extends DbObjectTestBase {

    Table table;
    User user;
    Role role;
    Right right;

    @Test
    public void run() {
        init();

        grantRight();
        revokeRight();

        grantRole();
        revokeRole();
    }

    void init() {
        executeUpdate("DROP TABLE IF EXISTS RightTest_t1");
        executeUpdate("CREATE TABLE IF NOT EXISTS RightTest_t1 (f1 int)");

        executeUpdate("DROP USER IF EXISTS RightTest_u1");
        executeUpdate("CREATE USER IF NOT EXISTS RightTest_u1 PASSWORD 'abc'");

        executeUpdate("DROP ROLE IF EXISTS RightTest_r1");
        executeUpdate("CREATE ROLE IF NOT EXISTS RightTest_r1");

        Schema schema = db.findSchema(session, Constants.SCHEMA_MAIN);
        assertNotNull(schema);
        table = schema.findTableOrView(session, "RightTest_t1");
        assertNotNull(table);
        user = findUser("RightTest_u1");
        assertNotNull(user);
        role = findRole("RightTest_r1");
        assertNotNull(role);
    }

    void grantRight() {
        executeUpdate("GRANT SELECT,DELETE,INSERT ON RightTest_t1 TO RightTest_u1");
        right = user.getRightForObject(table);
        assertNotNull(right);

        executeUpdate("GRANT SELECT,DELETE,INSERT ON RightTest_t1 TO RightTest_r1");
        right = role.getRightForObject(table);
        assertNotNull(right);

        executeUpdate("GRANT UPDATE ON RightTest_t1 TO PUBLIC");
        role = findRole("PUBLIC");
        assertNotNull(role);
        right = role.getRightForObject(table);
        assertNotNull(right);

        assertEquals(3, db.getAllRights().size());

        try {
            executeUpdate("GRANT SELECT, RightTest_r1 ON RightTest_t1 TO RightTest_u1");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.ROLES_AND_RIGHT_CANNOT_BE_MIXED);
        }

        executeUpdate("GRANT ALTER ANY SCHEMA TO RightTest_u1");
        assertTrue(user.hasRight(null, Right.ALTER_ANY_SCHEMA));
    }

    void reset() {
        user = findUser("RightTest_u1");
        assertNotNull(user);
        role = findRole("RightTest_r1");
        assertNotNull(role);
    }

    void revokeRight() {
        reset();
        executeUpdate("REVOKE SELECT,DELETE,INSERT ON RightTest_t1 FROM RightTest_u1");
        right = user.getRightForObject(table);
        assertNull(right);

        executeUpdate("REVOKE SELECT,DELETE,INSERT ON RightTest_t1 FROM RightTest_r1");
        right = role.getRightForObject(table);
        assertNull(right);
    }

    void grantRole() {
        reset();

        executeUpdate("GRANT RightTest_r1 TO RightTest_u1");
        right = user.getRightForRole(role);
        assertNotNull(right);

        executeUpdate("CREATE ROLE IF NOT EXISTS RightTest_r2");
        executeUpdate("GRANT RightTest_r1 TO RightTest_r2");
        try {
            executeUpdate("GRANT RightTest_r2 TO RightTest_r1");
            fail();
        } catch (Exception e) {
            assertException(e, ErrorCode.ROLE_ALREADY_GRANTED_1);
        }
    }

    void revokeRole() {
        reset();
        executeUpdate("REVOKE RightTest_r1 FROM RightTest_u1");
        right = user.getRightForRole(role);
        assertNull(right);
    }
}

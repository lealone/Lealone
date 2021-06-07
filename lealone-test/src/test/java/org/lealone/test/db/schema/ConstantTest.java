/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db.schema;

import org.junit.Test;
import org.lealone.db.session.ServerSession;
import org.lealone.test.db.DbObjectTestBase;
import org.lealone.transaction.Transaction;

public class ConstantTest extends DbObjectTestBase {
    @Test
    public void run() {
        create();
        drop();
    }

    void create() {
        executeUpdate("DROP CONSTANT IF EXISTS ConstantTest");
        session.setAutoCommit(false);
        executeUpdate("CREATE CONSTANT IF NOT EXISTS ConstantTest VALUE 10");
        assertNotNull(schema.findConstant(session, "ConstantTest"));

        testIsolationLevel();

        sql = "select ConstantTest";
        assertEquals(10, getInt(sql, 1));
        session.commit();
    }

    void drop() {
        session.setAutoCommit(false);
        executeUpdate("DROP CONSTANT IF EXISTS ConstantTest");
        assertNull(schema.findConstant(session, "ConstantTest"));
        session.commit();
        session.setAutoCommit(true);
    }

    void testIsolationLevel() {
        ServerSession s1 = createSession();
        s1.getTransaction().setIsolationLevel(Transaction.IL_READ_COMMITTED);
        // s1找不到，因为还没有提交
        assertNull(schema.findConstant(s1, "ConstantTest"));
        s1.close();

        ServerSession s2 = createSession();
        s2.getTransaction().setIsolationLevel(Transaction.IL_READ_UNCOMMITTED);
        // s2可以找到，虽然还没有提交，但是它的隔离级别是最宽松的
        assertNotNull(schema.findConstant(s2, "ConstantTest"));
        s2.close();
    }
}

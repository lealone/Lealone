/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db.schema;

import java.sql.Connection;
import java.util.ArrayList;

import org.junit.Test;
import org.lealone.db.api.Aggregate;
import org.lealone.db.schema.UserAggregate;
import org.lealone.db.value.Value;
import org.lealone.test.db.DbObjectTestBase;

public class UserAggregateTest extends DbObjectTestBase {
    @Test
    public void run() {
        int id = db.allocateObjectId();
        String className = MedianString.class.getName();
        String name = "MEDIAN";
        UserAggregate ua = new UserAggregate(schema, id, name, className, true);
        assertEquals(id, ua.getId());

        session.setAutoCommit(false);
        schema.add(session, ua, null);
        assertNotNull(schema.findAggregate(session, name));

        // ua.removeChildrenAndResources(session); //会触发invalidate

        String sql = "SELECT " + name + "(X) FROM SYSTEM_RANGE(1, 5)";
        assertEquals(3, getInt(sql, 1));

        schema.remove(session, ua, null);
        assertNull(schema.findAggregate(session, name));

        // 测试SQL
        // -----------------------------------------------
        sql = "CREATE FORCE AGGREGATE IF NOT EXISTS " + name + " FOR \"" + className + "\"";
        executeUpdate(sql);
        assertNotNull(schema.findAggregate(session, name));

        sql = "SELECT " + name + "(X) FROM SYSTEM_RANGE(1, 5)";
        assertEquals(3, getInt(sql, 1));

        sql = "DROP AGGREGATE " + name;
        executeUpdate(sql);
        assertNull(schema.findAggregate(session, name));
        session.commit();
        session.setAutoCommit(true);
    }

    public static class MedianString implements Aggregate {

        private final ArrayList<String> list = new ArrayList<>();

        @Override
        public void add(Object value) {
            list.add(value.toString());
        }

        @Override
        public Object getResult() {
            return list.get(list.size() / 2);
        }

        @Override
        public int getInternalType(int[] inputType) {
            return Value.STRING;
        }

        @Override
        public void init(Connection conn) {
            // nothing to do
        }
    }
}

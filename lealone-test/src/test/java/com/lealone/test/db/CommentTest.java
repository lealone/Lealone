/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db;

import org.junit.Test;

import com.lealone.db.Comment;
import com.lealone.db.Constants;
import com.lealone.db.auth.Role;
import com.lealone.db.table.Table;

public class CommentTest extends DbObjectTestBase {
    @Test
    public void run() {
        executeUpdate("CREATE TABLE IF NOT EXISTS CommentTest (f1 int)");
        executeUpdate("CREATE ROLE IF NOT EXISTS myrole");

        String dbName = DB_NAME;
        if (db.getSettings().databaseToUpper)
            dbName = dbName.toUpperCase();

        executeUpdate("COMMENT ON COLUMN " + dbName + "." + Constants.SCHEMA_MAIN
                + ".CommentTest.f1 IS 'column comment'");
        executeUpdate("COMMENT ON TABLE " + Constants.SCHEMA_MAIN + ".CommentTest IS 'table comment'");

        executeUpdate("COMMENT ON ROLE myrole IS 'role comment'");

        Table table = db.findSchema(session, Constants.SCHEMA_MAIN).findTableOrView(session,
                "CommentTest".toUpperCase());
        Comment comment = db.findComment(session, table);
        assertNull(comment); // 表的Comment并没有存到Database类的comments字段中

        Role role = db.findRole(session, "myrole");
        comment = db.findComment(session, role);
        assertNotNull(comment);
        assertNull(comment.getComment()); // Comment的comment是null
        assertEquals("role comment", comment.getCommentText());

        executeUpdate("COMMENT ON ROLE myrole IS NULL");
        comment = db.findComment(session, role);
        assertNull(comment);

        executeUpdate("DROP ROLE IF EXISTS myrole");
        executeUpdate("DROP TABLE IF EXISTS CommentTest");
    }
}

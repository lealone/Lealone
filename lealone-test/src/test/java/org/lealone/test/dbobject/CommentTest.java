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
package org.lealone.test.dbobject;

import org.junit.Test;
import org.lealone.dbobject.Comment;
import org.lealone.dbobject.Role;
import org.lealone.dbobject.table.Table;
import org.lealone.engine.Constants;

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

        Table table = db.findSchema(Constants.SCHEMA_MAIN).findTableOrView(session, "CommentTest".toUpperCase());
        Comment comment = db.findComment(table);
        assertNull(comment); //表的Comment并没有存到Database类的comments字段中

        Role role = db.findRole("myrole");
        comment = db.findComment(role);
        assertNotNull(comment);
        assertNull(comment.getComment()); //Comment的comment是null
        assertEquals("role comment", comment.getCommentText());

        executeUpdate("DROP ROLE IF EXISTS myrole");
        executeUpdate("DROP TABLE IF EXISTS CommentTest");
    }
}

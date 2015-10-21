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
package org.lealone.test.db.index;

import org.junit.Test;
import org.lealone.db.Constants;
import org.lealone.db.index.Index;
import org.lealone.db.table.Table;
import org.lealone.test.db.DbObjectTestBase;

public class IndexTest extends DbObjectTestBase {

    @Test
    public void run() {
        create();
        alter();
        drop();
    }

    void create() {
        executeUpdate("DROP TABLE IF EXISTS CreateIndexTest");
        executeUpdate("CREATE TABLE IF NOT EXISTS CreateIndexTest (f1 int NOT NULL, f2 int, f3 int)");

        Table table = schema.findTableOrView(session, "CreateIndexTest");
        assertNotNull(table);

        executeUpdate("CREATE PRIMARY KEY HASH ON CreateIndexTest(f1)");
        Index index = table.findPrimaryKey();
        assertNotNull(index);
        String indexName = index.getName();
        assertTrue(indexName.startsWith(Constants.PREFIX_PRIMARY_KEY));

        executeUpdate("DROP INDEX IF EXISTS " + indexName);
        index = schema.findIndex(session, indexName);
        assertNull(index);

        executeUpdate("CREATE PRIMARY KEY HASH IF NOT EXISTS idx0 ON CreateIndexTest(f1)");
        index = schema.findIndex(session, "idx0");
        assertNotNull(index);

        executeUpdate("CREATE UNIQUE HASH INDEX IF NOT EXISTS idx1 ON CreateIndexTest(f2)");
        index = schema.findIndex(session, "idx1");
        assertNotNull(index);

        executeUpdate("CREATE INDEX IF NOT EXISTS idx2 ON CreateIndexTest(f3)");
        index = schema.findIndex(session, "idx2");
        assertNotNull(index);
    }

    void alter() {
        executeUpdate("ALTER INDEX idx2 RENAME TO idx22");

        Index index = schema.findIndex(session, "idx22");
        assertNotNull(index);
        index = schema.findIndex(session, "idx2");
        assertNull(index);
    }

    void drop() {
        executeUpdate("DROP INDEX IF EXISTS idx22");
        Index index = schema.findIndex(session, "idx22");
        assertNull(index);
    }
}

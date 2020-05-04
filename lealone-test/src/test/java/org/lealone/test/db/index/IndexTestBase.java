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

import java.util.ArrayList;

import org.lealone.db.index.Index;
import org.lealone.db.table.Table;
import org.lealone.test.db.DbObjectTestBase;

public abstract class IndexTestBase extends DbObjectTestBase {

    protected void assertFound(String tableName, String indexName) {
        Table table = schema.findTableOrView(session, tableName);
        assertNotNull(table);
        Index index = schema.findIndex(session, indexName);
        assertNotNull(index);
        ArrayList<Index> indexes = table.getIndexes();
        assertTrue(indexes.contains(index));
    }

    protected void assertNotFound(String tableName, String indexName) {
        Table table = schema.findTableOrView(session, tableName);
        assertNotNull(table);
        Index index = schema.findIndex(session, indexName);
        assertNull(index);
        ArrayList<Index> indexes = table.getIndexes();
        assertFalse(indexes.contains(index));
    }
}

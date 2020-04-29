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
package org.lealone.test.db.table;

import java.util.ArrayList;

import org.junit.Test;
import org.lealone.db.schema.Schema;
import org.lealone.db.table.MetaTable;
import org.lealone.db.table.Table;
import org.lealone.test.db.DbObjectTestBase;

public class MetaTableTest extends DbObjectTestBase {
    @Test
    public void run() {
        String infoSchemaName = "INFORMATION_SCHEMA";
        Schema infoSchema = db.findSchema(infoSchemaName);
        ArrayList<Table> tables = infoSchema.getAllTablesAndViews();
        assertEquals(MetaTable.getMetaTableTypeCount(), tables.size());

        for (Table table : tables) {
            p("table name: " + table.getName());
            p("============================");
            sql = "select * from " + infoSchemaName + "." + table.getName();
            printResultSet(sql);
        }

        String[] tableNames = { "TABLE_TYPES" };
        tableNames[0] = "CATALOGS";
        tableNames[0] = "SETTINGS";
        tableNames[0] = "IN_DOUBT";
        tableNames[0] = "TABLES";
        tableNames[0] = "HELP";
        for (String tableName : tableNames) {
            if (tableName == null)
                continue;
            p("table name: " + tableName);
            p("============================");
            sql = "select * from " + infoSchemaName + "." + tableName;
            printResultSet(sql);
        }
    }
}

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
package org.lealone.orm;

import org.lealone.db.ServerSession;

public class Table {

    private final org.lealone.db.table.Table dbTable;
    private final Database db;

    public Table(String url, String tableName) {
        db = new Database(url);
        dbTable = db.getDbTable(tableName);
        dbTable.getTemplateRow();
    }

    Table(Database db, org.lealone.db.table.Table dbTable) {
        this.db = db;
        this.dbTable = dbTable;
    }

    public void save(Object bean) {
        dbTable.getColumns();
    }

    public void save(Object... beans) {
        for (Object o : beans) {
            save(o);
        }
    }

    public boolean delete(Object bean) {
        return false;
    }

    org.lealone.db.table.Table getDbTable() {
        return dbTable;
    }

    ServerSession getSession() {
        return db.getSession();
    }

    public Database getDatabase() {
        return db;
    }
}

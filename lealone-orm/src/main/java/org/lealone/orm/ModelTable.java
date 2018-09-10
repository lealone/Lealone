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

import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.ServerSessionFactory;
import org.lealone.db.table.Table;

public class ModelTable {

    private final String url;
    private final String databaseName;
    private final String schemaName;
    private final String tableName;

    private Table table;
    private ServerSession session;

    public ModelTable(String databaseName, String schemaName, String tableName) {
        this(null, databaseName, schemaName, tableName);
    }

    public ModelTable(String url, String databaseName, String schemaName, String tableName) {
        this.url = url;
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        if (url != null)
            attachToTable();
    }

    String getDatabaseName() {
        return databaseName;
    }

    String getSchemaName() {
        return schemaName;
    }

    String getTableName() {
        return tableName;
    }

    Table getTable() {
        attachToTable();
        return table;
    }

    ServerSession getSession() {
        attachToTable();
        return session;
    }

    Database getDatabase() {
        attachToTable();
        return table.getDatabase();
    }

    public ModelTable copy() {
        return new ModelTable(url, databaseName, schemaName, tableName);
    }

    // 可能是延迟关联到Table
    private void attachToTable() {
        if (table == null) {
            String url = System.getProperty("lealone.jdbc.url");
            if (url == null) {
                throw new RuntimeException("'lealone.jdbc.url' must be set");
            }

            session = ServerSessionFactory.getInstance().createSession(url);
            Database db = session.getDatabase();

            // if (db.getSettings().databaseToUpper) {
            // tableName = tableName.toUpperCase();
            // }
            // int dotPos = tableName.indexOf('.');
            // String schemaName = Constants.SCHEMA_MAIN;
            // if (dotPos > -1) {
            // schemaName = tableName.substring(0, dotPos);
            // tableName = tableName.substring(dotPos + 1);
            // }
            table = db.getSchema(schemaName).getTableOrView(session, tableName);
        }
    }

}

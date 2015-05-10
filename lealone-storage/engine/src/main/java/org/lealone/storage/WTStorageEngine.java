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
package org.lealone.storage;

import java.io.File;
import java.util.HashMap;

import org.lealone.command.ddl.CreateTableData;
import org.lealone.dbobject.table.MVTable;
import org.lealone.dbobject.table.Table;
import org.lealone.engine.Database;

import com.wiredtiger.db.Connection;
import com.wiredtiger.db.wiredtiger;

public class WTStorageEngine extends MVStorageEngine implements TransactionStorageEngine {
    public static final String NAME = "WT";
    private static final HashMap<String, Connection> connections = new HashMap<String, Connection>(1);

    //见StorageEngineManager.StorageEngineService中的注释
    public WTStorageEngine() {
        StorageEngineManager.registerStorageEngine(this);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Table createTable(CreateTableData data) {
        Database db = data.session.getDatabase();
        String dbName = db.getName();
        Connection conn = connections.get(dbName);
        if (conn == null) {
            synchronized (connections) {
                if (connections.get(dbName) == null) {
                    conn = createConnection(dbName);
                    connections.put(dbName, conn);

                    Store store = init(db, new WTMapBuilder(conn.open_session(null)), getTransactionEngine());
                    stores.put(db.getName(), store);
                    db.setStorageEngine(this);
                    db.setTransactionEngine(store.getTransactionEngine());
                    db.setLobStorage(new LobStorageMap(db));
                }
            }
        }
        MVTable table = new MVTable(data, this);
        table.init(data.session);
        return table;
    }

    @Override
    public void close(Database db) {
        super.close(db);
        Connection conn = connections.remove(db.getName());
        if (conn != null) {
            conn.close(null);
        }
    }

    private Connection createConnection(String dbName) {
        File home = new File(dbName);
        if (!home.exists())
            home.mkdir();

        return wiredtiger.open(dbName, "create");
    }
}

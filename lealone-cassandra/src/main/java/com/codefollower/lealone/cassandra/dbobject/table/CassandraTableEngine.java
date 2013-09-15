/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.codefollower.lealone.cassandra.dbobject.table;

import com.codefollower.lealone.api.TableEngine;
import com.codefollower.lealone.command.ddl.CreateTableData;
import com.codefollower.lealone.dbobject.table.MemoryTable;
import com.codefollower.lealone.dbobject.table.TableBase;
import com.codefollower.lealone.dbobject.table.TableEngineManager;
import com.codefollower.lealone.engine.Database;

/**
 * 
 * 用于支持标准的CREATE TABLE语句。
 *
 */
public class CassandraTableEngine implements TableEngine {
    public static final String NAME = "CASSANDRA";
    private static final CassandraTableEngine INSTANCE = new CassandraTableEngine();
    static {
        TableEngineManager.registerTableEngine(INSTANCE);
    }

    @Override
    public TableBase createTable(CreateTableData data) {
        Database db = data.session.getDatabase();
        if (!data.isHidden && !data.temporary && data.id > 0 //
                && !db.isPersistent() && !db.getShortName().toLowerCase().startsWith("management_db_"))
            return new CassandraTable(data);
        else
            return new MemoryTable(data);
    }

    @Override
    public String getName() {
        return NAME;
    }

}

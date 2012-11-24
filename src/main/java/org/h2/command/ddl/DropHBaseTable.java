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
package org.h2.command.ddl;

import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKTableReadOnly;
import org.h2.command.CommandInterface;
import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;

public class DropHBaseTable extends SchemaCommand {
    private Table table;
    private boolean ifExists;
    private String tableName;

    public DropHBaseTable(Session session, Schema schema, String tableName) {
        super(session, schema);
        this.tableName = tableName;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_TABLE;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    private void prepareDrop() {
        table = getSchema().findTableOrView(session, tableName);
        if (table == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
            }
        } else {
            session.getUser().checkRight(table, Right.ALL);
            if (!table.canDrop()) {
                throw DbException.get(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
            }
            table.lock(session, true, true);
        }
    }

    private void executeDrop() {
        // need to get the table again, because it may be dropped already
        // meanwhile (dependent object, or same object)
        table = getSchema().findTableOrView(session, tableName);

        if (table != null) {
            try {
                HMaster master = session.getMaster();
                if (master != null) {
                    master.disableTable(Bytes.toBytes(tableName));
                    while (true) {
                        if (ZKTableReadOnly.isDisabledTable(master.getZooKeeperWatcher(), tableName))
                            break;
                        Thread.sleep(100);
                    }
                    master.deleteTable(Bytes.toBytes(tableName));
                }
            } catch (Exception e) {
                throw DbException.convert(e); //Failed to HMaster.deleteTable
            }
            table.setModified();
            Database db = session.getDatabase();
            db.lockMeta(session);
            db.removeSchemaObject(session, table);
        }
    }

    public int update() {
        session.commit(true);
        prepareDrop();
        executeDrop();

        return 0;
    }
}

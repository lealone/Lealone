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
package com.codefollower.lealone.hbase.zookeeper;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import com.codefollower.lealone.hbase.metadata.DDLRedoTable;
import com.codefollower.lealone.message.DbException;

public class DDLRedoTableTracker extends ZooKeeperListener {
    private final DDLRedoTable table;
    private int redoPos;

    public DDLRedoTableTracker(ZooKeeperWatcher watcher, DDLRedoTable table) {
        super(watcher);
        this.table = table;
    }

    public void start() {
        watcher.registerListener(this);
        redoPos = getRedoPos(true);
    }

    public synchronized void refresh() {
        int newRedoPos = getRedoPos(true);
        if (newRedoPos != redoPos) {
            int startPos = redoPos;
            int stopPos = newRedoPos;

            if (newRedoPos < startPos) { //Master重新计数了
                startPos = 1;
            }
            try {
                table.redoRecords(startPos, stopPos);
                redoPos = newRedoPos;
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
    }

    @Override
    public void nodeDataChanged(String path) {
        if (path.equals(ZooKeeperAdmin.DDL_REDO_TABLE_NODE)) {
            refresh();
        }
    }

    public int getRedoPos(boolean watch) {
        try {
            byte[] data = null;
            if (watch)
                data = ZKUtil.getDataAndWatch(watcher, ZooKeeperAdmin.DDL_REDO_TABLE_NODE);
            else
                data = ZKUtil.getData(watcher, ZooKeeperAdmin.DDL_REDO_TABLE_NODE);
            if (data != null && data.length > 0) {
                return Bytes.toInt(data);
            }
            return 1;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }
}
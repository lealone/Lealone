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
package com.codefollower.lealone.hbase.command.dml;

import com.codefollower.lealone.command.dml.Update;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.command.HBasePrepared;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.value.Value;

public class HBaseUpdate extends Update implements HBasePrepared {
    private String regionName;
    private boolean isBatch = false;

    public HBaseUpdate(Session session) {
        super(session);
    }

    @Override
    public void prepare() {
        if (session.getAutoCommit()) {
            session.setAutoCommit(false);
            isBatch = true;
        }
        super.prepare();
        tableFilter.setPrepared(this);
    }

    @Override
    public int update() {
        try {
            int updateCount = super.update();

            if (isBatch)
                session.commit(false);
            return updateCount;
        } catch (Exception e) {
            if (isBatch)
                session.rollback();
            throw DbException.convert(e);
        } finally {
            if (isBatch)
                session.setAutoCommit(true);
        }
    }

    @Override
    public boolean isDistributedSQL() {
        return true;
    }

    @Override
    public String getRowKey() {
        Value rowKey = getStartRowKeyValue();
        if (rowKey != null)
            rowKey.getString();
        return null;
    }

    @Override
    public String getTableName() {
        return tableFilter.getTable().getName();
    }

    @Override
    public byte[] getTableNameAsBytes() {
        return ((HBaseTable) tableFilter.getTable()).getTableNameAsBytes();
    }

    @Override
    public Value getStartRowKeyValue() {
        SearchRow start = tableFilter.getStartSearchRow();
        if (start != null)
            return start.getRowKey();
        return null;
    }

    @Override
    public Value getEndRowKeyValue() {
        SearchRow end = tableFilter.getEndSearchRow();
        if (end != null)
            return end.getRowKey();
        return null;
    }

    @Override
    public String getRegionName() {
        return regionName;
    }

    @Override
    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }
}

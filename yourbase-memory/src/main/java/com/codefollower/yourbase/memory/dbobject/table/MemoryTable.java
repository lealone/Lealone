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
package com.codefollower.yourbase.memory.dbobject.table;

import java.util.ArrayList;

import com.codefollower.yourbase.command.ddl.CreateTableData;
import com.codefollower.yourbase.dbobject.index.Index;
import com.codefollower.yourbase.dbobject.index.IndexType;
import com.codefollower.yourbase.dbobject.index.ScanIndex;
import com.codefollower.yourbase.dbobject.table.IndexColumn;
import com.codefollower.yourbase.dbobject.table.TableBase;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.result.Row;

public class MemoryTable extends TableBase {
    private Index scanIndex;

    public MemoryTable(CreateTableData data) {
        super(data);
        scanIndex = new ScanIndex(this, data.id, IndexColumn.wrap(getColumns()), IndexType.createScan(data.persistData));
    }

    @Override
    public void lock(Session session, boolean exclusive, boolean force) {

    }

    @Override
    public void close(Session session) {

    }

    @Override
    public void unlock(Session s) {

    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment) {

        return null;
    }

    @Override
    public void removeRow(Session session, Row row) {

    }

    @Override
    public void truncate(Session session) {

    }

    @Override
    public void addRow(Session session, Row row) {

    }

    @Override
    public void checkSupportAlter() {

    }

    @Override
    public String getTableType() {

        return null;
    }

    @Override
    public Index getScanIndex(Session session) {

        return scanIndex;
    }

    @Override
    public Index getUniqueIndex() {

        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {

        return null;
    }

    @Override
    public boolean isLockedExclusively() {

        return false;
    }

    @Override
    public long getMaxDataModificationId() {

        return 0;
    }

    @Override
    public boolean isDeterministic() {

        return false;
    }

    @Override
    public boolean canGetRowCount() {

        return false;
    }

    @Override
    public boolean canDrop() {

        return false;
    }

    @Override
    public long getRowCount(Session session) {

        return rowCount;
    }

    @Override
    public long getRowCountApproximation() {

        return 0;
    }

    @Override
    public long getDiskSpaceUsed() {

        return 0;
    }

    @Override
    public void checkRename() {

    }

    @Override
    public Row getRow(Session session, long key) {
        return scanIndex.getRow(session, key);
    }

}

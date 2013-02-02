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
package com.codefollower.yourbase.dbobject.table;

import java.util.ArrayList;

import com.codefollower.yourbase.command.ddl.CreateTableData;
import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.dbobject.index.HashIndex;
import com.codefollower.yourbase.dbobject.index.Index;
import com.codefollower.yourbase.dbobject.index.IndexType;
import com.codefollower.yourbase.dbobject.index.MultiVersionIndex;
import com.codefollower.yourbase.dbobject.index.NonUniqueHashIndex;
import com.codefollower.yourbase.dbobject.index.ScanIndex;
import com.codefollower.yourbase.dbobject.index.TreeIndex;
import com.codefollower.yourbase.dbobject.table.IndexColumn;
import com.codefollower.yourbase.dbobject.table.TableBase;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.result.Row;
import com.codefollower.yourbase.util.New;

public class MemoryTable extends TableBase {
    private Index scanIndex;
    private final ArrayList<Index> indexes = New.arrayList();

    public MemoryTable(CreateTableData data) {
        super(data);
        scanIndex = new ScanIndex(this, data.id, IndexColumn.wrap(getColumns()), IndexType.createScan(data.persistData));
        indexes.add(scanIndex);
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
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
                }
                column.setPrimaryKey(true);
            }
        }
        boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
        if (!isSessionTemporary) {
            database.lockMeta(session);
        }
        Index index;
        if (indexType.isHash() && cols.length <= 1) {
            if (indexType.isUnique()) {
                index = new HashIndex(this, indexId, indexName, cols, indexType);
            } else {
                index = new NonUniqueHashIndex(this, indexId, indexName, cols, indexType);
            }
        } else {
            index = new TreeIndex(this, indexId, indexName, cols, indexType);
        }
        if (database.isMultiVersion()) {
            index = new MultiVersionIndex(index, this);
        }
        index.setTemporary(isTemporary());
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            if (isSessionTemporary) {
                session.addLocalTempTableIndex(index);
            } else {
                database.addSchemaObject(session, index);
            }
        }
        indexes.add(index);
        setModified();
        return index;
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
        return Table.TABLE;
    }

    @Override
    public Index getScanIndex(Session session) {
        return scanIndex;
    }

    @Override
    public Index getUniqueIndex() {
        for (Index idx : indexes) {
            if (idx.getIndexType().isUnique()) {
                return idx;
            }
        }
        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
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
        return true;
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
        return scanIndex.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return scanIndex.getDiskSpaceUsed();
    }

    @Override
    public void checkRename() {
    }

    @Override
    public Row getRow(Session session, long key) {
        return scanIndex.getRow(session, key);
    }

}

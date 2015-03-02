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
package org.lealone.wiredtiger.dbobject.table;

import java.util.ArrayList;

import org.lealone.api.ErrorCode;
import org.lealone.command.ddl.CreateTableData;
import org.lealone.dbobject.index.Index;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.Table;
import org.lealone.dbobject.table.TableBase;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.SortOrder;
import org.lealone.value.DataType;
import org.lealone.value.Value;
import org.lealone.wiredtiger.dbobject.index.WiredTigerDelegateIndex;
import org.lealone.wiredtiger.dbobject.index.WiredTigerPrimaryIndex;
import org.lealone.wiredtiger.dbobject.index.WiredTigerSecondaryIndex;

public class WiredTigerTable extends TableBase {
    private final WiredTigerPrimaryIndex primaryIndex;

    public WiredTigerTable(CreateTableData data) {
        super(data);
        for (Column col : getColumns()) {
            if (DataType.isLargeObject(col.getType())) {
                containsLargeObject = true;
            }
        }
        primaryIndex = new WiredTigerPrimaryIndex(data.session.getDatabase(), this, getId(), IndexColumn.wrap(getColumns()),
                IndexType.createScan(true));
        indexes.add(primaryIndex);
        scanIndex = primaryIndex;
    }

    @Override
    public void close(Session session) {

    }

    @Override
    public void unlock(Session s) {

    }

    @Override
    public void truncate(Session session) {

    }

    @Override
    public void checkSupportAlter() {

    }

    @Override
    public String getTableType() {
        return Table.EXTERNAL_STORAGE_ENGINE;
    }

    @Override
    public Index getScanIndex(Session session) {
        return scanIndex;
    }

    @Override
    public Index getUniqueIndex() {
        return scanIndex;
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
        return false;
    }

    @Override
    public boolean canGetRowCount() {
        return false;
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public long getRowCount(Session session) {
        return rowCount;
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public boolean supportsSharding() {
        return true;
    }

    @Override
    public boolean supportsColumnFamily() {
        return true;
    }

    @Override
    public boolean supportsAlterColumnWithCopyData() {
        return false;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public void addRow(Session session, Row row) {
        lastModificationId = database.getNextModificationDataId();
        int i = 0;
        try {
            for (int size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                index.add(session, row);
                //checkRowCount(session, index, 1);
            }
            rowCount++;
        } catch (Throwable e) {//TODO rollbackToSavepoint
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    @Override
    public void removeRow(Session session, Row row) {
        lastModificationId = database.getNextModificationDataId();
        int i = indexes.size() - 1;
        try {
            for (; i >= 0; i--) {
                Index index = indexes.get(i);
                index.remove(session, row);
            }
            rowCount--;
        } catch (Throwable e) {
            //TODO rollbackToSavepoint
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment) {

        boolean isDelegateIndex = false;
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
                }
                column.setPrimaryKey(true);
                column.setRowKeyColumn(true);
            }

            if (cols.length == 1) {
                isDelegateIndex = true;
            }
        }
        boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
        if (!isSessionTemporary) {
            database.lockMeta(session);

        }
        Index index;
        int mainIndexColumn;
        mainIndexColumn = getMainIndexColumn(indexType, cols);
        if (mainIndexColumn != -1)
            primaryIndex.setMainIndexColumn(mainIndexColumn);
        if (isDelegateIndex)
            index = new WiredTigerDelegateIndex(this, indexId, indexName, cols, primaryIndex, indexType);
        else
            index = new WiredTigerSecondaryIndex(this, indexId, indexName, cols, indexType);

        index.setTemporary(isTemporary());
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            if (isSessionTemporary) {
                session.addLocalTempTableIndex(index);
            } else {
                database.addSchemaObject(session, index);
            }
        }
        setModified();
        indexes.add(index);
        return index;
    }

    private int getMainIndexColumn(IndexType indexType, IndexColumn[] cols) {
        if (primaryIndex.getMainIndexColumn() != -1) {
            return -1;
        }
        if (!indexType.isPrimaryKey() || cols.length != 1) {
            return -1;
        }
        IndexColumn first = cols[0];
        if (first.sortType != SortOrder.ASCENDING) {
            return -1;
        }
        switch (first.column.getType()) {
        case Value.BYTE:
        case Value.SHORT:
        case Value.INT:
        case Value.LONG:
            break;
        default:
            return -1;
        }
        return first.column.getColumnId();
    }
}

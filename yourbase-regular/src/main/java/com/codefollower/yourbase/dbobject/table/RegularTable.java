/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.dbobject.table;

import com.codefollower.yourbase.command.ddl.CreateTableData;
import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.dbobject.index.HashIndex;
import com.codefollower.yourbase.dbobject.index.Index;
import com.codefollower.yourbase.dbobject.index.IndexType;
import com.codefollower.yourbase.dbobject.index.MultiVersionIndex;
import com.codefollower.yourbase.dbobject.index.NonUniqueHashIndex;
import com.codefollower.yourbase.dbobject.index.PageBtreeIndex;
import com.codefollower.yourbase.dbobject.index.PageDataIndex;
import com.codefollower.yourbase.dbobject.index.PageDelegateIndex;
import com.codefollower.yourbase.dbobject.index.ScanIndex;
import com.codefollower.yourbase.dbobject.index.TreeIndex;
import com.codefollower.yourbase.engine.RegularDatabase;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.result.SortOrder;
import com.codefollower.yourbase.value.Value;

/**
 * Most tables are an instance of this class. For this table, the data is stored
 * in the database. The actual data is not kept here, instead it is kept in the
 * indexes. There is at least one index, the scan index.
 */
public class RegularTable extends TableBase {
    private final PageDataIndex mainIndex;

    public RegularTable(CreateTableData data) {
        super(data);
        if (data.persistData && database.isPersistent()) {
            mainIndex = new PageDataIndex(this, data.id, IndexColumn.wrap(getColumns()), IndexType.createScan(data.persistData),
                    data.create, data.session);
            scanIndex = mainIndex;
        } else {
            mainIndex = null;
            scanIndex = new ScanIndex(this, data.id, IndexColumn.wrap(getColumns()), IndexType.createScan(data.persistData));
        }
        indexes.add(scanIndex);
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment) {
        RegularDatabase database = (RegularDatabase) this.database;
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
        if (isPersistIndexes() && indexType.isPersistent()) {
            int mainIndexColumn;
            if (database.isStarting() && database.getPageStore().getRootPageId(indexId) != 0) {
                mainIndexColumn = -1;
            } else if (!database.isStarting() && mainIndex.getRowCount(session) != 0) {
                mainIndexColumn = -1;
            } else {
                mainIndexColumn = getMainIndexColumn(indexType, cols);
            }
            if (mainIndexColumn != -1) {
                mainIndex.setMainIndexColumn(mainIndexColumn);
                index = new PageDelegateIndex(this, indexId, indexName, indexType, mainIndex, create, session);
            } else {
                index = new PageBtreeIndex(this, indexId, indexName, cols, indexType, create, session);
            }
        } else {
            if (indexType.isHash() && cols.length <= 1) {
                if (indexType.isUnique()) {
                    index = new HashIndex(this, indexId, indexName, cols, indexType);
                } else {
                    index = new NonUniqueHashIndex(this, indexId, indexName, cols, indexType);
                }
            } else {
                index = new TreeIndex(this, indexId, indexName, cols, indexType);
            }
        }
        if (database.isMultiVersion()) {
            index = new MultiVersionIndex(index, this);
        }
        rebuildIfNeed(session, index, indexName);
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

    private int getMainIndexColumn(IndexType indexType, IndexColumn[] cols) {
        if (mainIndex.getMainIndexColumn() != -1) {
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

    @Override
    public void checkRowCount(Session session, Index index, int offset) {
        if (!(index instanceof PageDelegateIndex)) {
            super.checkRowCount(session, index, offset);
        }
    }
}

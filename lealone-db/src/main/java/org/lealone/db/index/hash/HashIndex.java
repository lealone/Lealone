/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.index.hash;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.IndexBase;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexConditionType;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;

public abstract class HashIndex extends IndexBase {

    /**
     * The index of the indexed column.
     */
    protected final int indexColumn;

    protected HashIndex(Table table, int id, String indexName, IndexType indexType, IndexColumn[] columns) {
        super(table, id, indexName, indexType, columns);
        this.indexColumn = columns[0].column.getColumnId();
        reset();
    }

    protected abstract void reset();

    @Override
    public void truncate(ServerSession session) {
        reset();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public void close(ServerSession session) {
        // nothing to do
    }

    @Override
    public void remove(ServerSession session) {
        // nothing to do
    }

    @Override
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexConditionType.EQUALITY) != IndexConditionType.EQUALITY) {
                return Long.MAX_VALUE;
            }
        }
        return 2;
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(ServerSession session, boolean first) {
        throw DbException.getUnsupportedException("HASH");
    }

    @Override
    public boolean canScan() {
        return false;
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    @Override
    public boolean isInMemory() {
        return true;
    }
}

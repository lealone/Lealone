/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import com.lealone.db.index.Cursor;
import com.lealone.db.index.IndexBase;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexType;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.StandardTable;
import com.lealone.transaction.TransactionMap;

public abstract class StandardDataIndex<K, V> extends IndexBase {

    protected final StandardTable table;
    protected final String mapName;
    protected TransactionMap<K, V> dataMap;

    protected StandardDataIndex(StandardTable table, int id, String name, IndexType indexType,
            IndexColumn[] indexColumns) {
        super(table, id, name, indexType, indexColumns);
        this.table = table;
        if (indexType.isScan())
            mapName = table.getMapNameForTable(id);
        else
            mapName = table.getMapNameForIndex(id);
    }

    @Override
    public StandardTable getTable() {
        return table;
    }

    public String getMapName() {
        return mapName;
    }

    public TransactionMap<K, V> getDataMap() {
        return dataMap;
    }

    public TransactionMap<K, V> getTransactionMap(ServerSession session) {
        if (session == null) {
            return dataMap;
        }
        return dataMap.getInstance(session.getTransaction());
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public void close(ServerSession session) {
        dataMap.close();
    }

    @Override
    public boolean isClosed() {
        return dataMap.isClosed();
    }

    @Override
    public void remove(ServerSession session) {
        TransactionMap<?, ?> map = getTransactionMap(session);
        if (!map.isClosed()) {
            map.remove();
        }
    }

    @Override
    public void truncate(ServerSession session) {
        getTransactionMap(session).clear();
    }

    @Override
    public long getDiskSpaceUsed() {
        return dataMap.getDiskSpaceUsed();
    }

    @Override
    public long getMemorySpaceUsed() {
        return dataMap.getMemorySpaceUsed();
    }

    // 标记类
    protected static abstract class StandardDataIndexCursor implements Cursor {
    }
}

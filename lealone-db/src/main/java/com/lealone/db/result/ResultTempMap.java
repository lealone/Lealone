/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.result;

import java.util.ArrayList;

import com.lealone.db.Database;
import com.lealone.db.index.standard.StandardDataType;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueLong;
import com.lealone.sql.IExpression;
import com.lealone.storage.Storage;
import com.lealone.storage.StorageMap;
import com.lealone.storage.StorageMapCursor;
import com.lealone.storage.type.StorageDataType;

// 单线程操作
public class ResultTempMap implements ResultExternal {

    private final ServerSession session;
    private final boolean distinct;
    private final SortOrder sort;
    private final int columnCount;
    private final ResultTempMap parent;

    private int rowCount;

    private boolean closed;
    private int childCount;

    private StorageMap<Value, ValueArray> storageMap;
    private StorageMapCursor<Value, ValueArray> resultCursor;
    private int[] sortColumnIndexes;

    ResultTempMap(ServerSession session, IExpression[] expressions, boolean distinct, SortOrder sort) {
        this.session = session;
        this.distinct = distinct;
        this.sort = sort;
        columnCount = expressions.length;
        parent = null;
        if (distinct) {
            Database db = session.getDatabase();
            StandardDataType type = new StandardDataType(db.getCompareMode(), getSortTypes());
            type.setKeyOnly(true);
            openMap(db, type, type);
        } else if (sort != null) {
            sortColumnIndexes = sort.getQueryColumnIndexes();
            int[] keySortTypes = new int[sortColumnIndexes.length + 1];
            for (int i = 0; i < sortColumnIndexes.length; i++) {
                keySortTypes[i] = sort.getSortTypes()[i];
            }
            keySortTypes[sortColumnIndexes.length] = SortOrder.ASCENDING;

            Database db = session.getDatabase();
            StandardDataType keyType = new StandardDataType(db.getCompareMode(), keySortTypes);
            StandardDataType valueType = new StandardDataType(db.getCompareMode(), getSortTypes());
            openMap(db, keyType, valueType);
        } else {
            Database db = session.getDatabase();
            StandardDataType keyType = new StandardDataType(db.getCompareMode(),
                    new int[] { SortOrder.ASCENDING });
            StandardDataType valueType = new StandardDataType(db.getCompareMode(), getSortTypes());
            openMap(db, keyType, valueType);
        }
    }

    private ResultTempMap(ResultTempMap parent) {
        this.session = parent.session;
        this.distinct = parent.distinct;
        this.sort = parent.sort;
        this.columnCount = parent.columnCount;
        this.parent = parent;
        this.rowCount = parent.rowCount;
        this.storageMap = parent.storageMap;
        this.sortColumnIndexes = parent.sortColumnIndexes;
        reset();
    }

    private void openMap(Database db, StorageDataType keyType, StorageDataType valueType) {
        Storage storage = db.getStorage(db.getDefaultStorageEngineName());
        String mapName = storage.nextTemporaryMapName();
        storageMap = storage.openMap(mapName, keyType, valueType, null);
    }

    private int[] getSortTypes() {
        int[] sortTypes = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }
        return sortTypes;
    }

    @Override
    public ResultExternal createShallowCopy() {
        if (parent != null) {
            return parent.createShallowCopy();
        }
        if (closed) {
            return null;
        }
        childCount++;
        return new ResultTempMap(this);
    }

    @Override
    public int removeDistinct(Value[] values) {
        ValueArray a = ValueArray.get(values);
        if (distinct) {
            if (storageMap.remove(a) != null)
                rowCount--;
        }
        return rowCount;
    }

    @Override
    public boolean containsDistinct(Value[] values) {
        ValueArray a = ValueArray.get(values);
        if (distinct) {
            return storageMap.containsKey(a);
        }
        return false;
    }

    @Override
    public int addRow(Value[] values) {
        ValueArray a = ValueArray.get(values);
        if (distinct) {
            if (storageMap.putIfAbsent(a, a) == null)
                rowCount++;
        } else {
            if (sortColumnIndexes != null) {
                Value[] key = new Value[sortColumnIndexes.length + 1];
                for (int i = 0; i < sortColumnIndexes.length; i++) {
                    key[i] = values[i];
                }
                key[sortColumnIndexes.length] = ValueLong.get(rowCount);
                storageMap.put(ValueArray.get(key), a);
            } else {
                storageMap.append(a);
            }
            rowCount++;
        }
        return rowCount;
    }

    @Override
    public int addRows(ArrayList<Value[]> rows) {
        // speeds up inserting, but not really needed:
        if (sort != null) {
            sort.sort(rows);
        }
        for (Value[] values : rows) {
            addRow(values);
        }
        return rowCount;
    }

    private void closeChild() {
        if (--childCount == 0 && closed) {
            dropMap();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (parent != null) {
            parent.closeChild();
        } else {
            if (childCount == 0) {
                dropMap();
            }
        }
    }

    private void dropMap() {
        if (storageMap == null) {
            return;
        }
        try {
            storageMap.remove();
        } finally {
            storageMap = null;
        }
    }

    @Override
    public void done() {
        // nothing to do
    }

    @Override
    public Value[] next() {
        if (resultCursor == null) {
            resultCursor = storageMap.cursor();
        }
        if (resultCursor.next()) {
            return resultCursor.getValue().getList();
        } else {
            return null;
        }
    }

    @Override
    public void reset() {
        resultCursor = null;
    }
}

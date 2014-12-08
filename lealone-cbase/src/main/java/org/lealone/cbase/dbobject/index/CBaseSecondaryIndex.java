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
package org.lealone.cbase.dbobject.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.api.ErrorCode;
import org.lealone.dbobject.index.BaseIndex;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.Table;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.result.SortOrder;
import org.lealone.value.Value;
import org.lealone.value.ValueArray;
import org.lealone.value.ValueLong;

public class CBaseSecondaryIndex extends BaseIndex {
    private final ConcurrentNavigableMap<Value, Row> rows = new ConcurrentSkipListMap<Value, Row>();
    private final int keyColumns;

    public CBaseSecondaryIndex(Table table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, indexName, columns, indexType);
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }

        // always store the row key in the map key,
        // even for unique indexes, as some of the index columns could be null
        keyColumns = columns.length + 1;
    }

    //    private static void checkIndexColumnTypes(IndexColumn[] columns) {
    //        for (IndexColumn c : columns) {
    //            int type = c.column.getType();
    //            if (type == Value.CLOB || type == Value.BLOB) {
    //                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1,
    //                        "Index on BLOB or CLOB column: " + c.column.getCreateSQL());
    //            }
    //        }
    //    }

    @Override
    public void close(Session session) {
    }

    private Value getKey(Value v) {
        v.compareMode = table.getCompareMode();
        return v;
    }

    @Override
    public void add(Session session, Row row) {
        ValueArray array = convertToKey(row);
        rows.put(getKey(array), row);
    }

    @Override
    public void remove(Session session, Row row) {
        ValueArray array = convertToKey(row);
        rows.remove(getKey(array));
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        ValueArray firstArray = convertToKey(first);
        ValueArray lastArray = convertToKey(last);
        if (lastArray != null) {
            lastArray.getList()[keyColumns - 1] = CBasePrimaryIndex.MAX;
        }
        return new CBaseSecondaryIndexCursor(rows.tailMap(getKey(firstArray)).entrySet().iterator(), lastArray, session
                .getTransaction().getTransactionId());
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        try {
            return 10 * getCostRangeIndex(masks, rows.size(), sortOrder);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public void remove(Session session) {
        rows.clear();
    }

    @Override
    public void truncate(Session session) {
        rows.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        Row row = (first ? rows.firstEntry().getValue() : rows.lastEntry().getValue());
        if (row == null) {
            return new CBaseSecondaryIndexCursor(Collections.<Entry<Value, Row>> emptyList().iterator(), null, session
                    .getTransaction().getTransactionId());
        }
        ValueArray array = convertToKey(row);
        HashMap<Value, Row> e = new HashMap<>(1);
        Value key = getKey(array);
        e.put(key, row);
        CBaseSecondaryIndexCursor c = new CBaseSecondaryIndexCursor(e.entrySet().iterator(), key, session.getTransaction()
                .getTransactionId());
        c.next();
        return c;
    }

    @Override
    public boolean needRebuild() {
        return rows.size() == 0;
    }

    @Override
    public long getRowCount(Session session) {
        return rows.size();
    }

    @Override
    public long getRowCountApproximation() {
        return rows.size();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public void checkRename() {
        //ok
    }

    private ValueArray convertToKey(SearchRow r) {
        if (r == null) {
            return null;
        }
        Value[] array = new Value[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            int idx = c.getColumnId();
            Value v = r.getValue(idx);
            if (v != null) {
                array[i] = v.convertTo(c.getType());
            }
        }
        array[keyColumns - 1] = ValueLong.get(r.getKey());
        return ValueArray.get(array);
    }

    class CBaseSecondaryIndexCursor implements Cursor {

        private final Iterator<Entry<Value, Row>> it;
        private final Value last;
        private Entry<Value, Row> current;
        private Row row;

        private final long transactionId;

        public CBaseSecondaryIndexCursor(Iterator<Entry<Value, Row>> it, Value last, long transactionId) {
            this.it = it;
            this.last = last;
            this.transactionId = transactionId;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    row = current.getValue();
                }
            }
            if (row != null)
                row.setTransactionId(transactionId);
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            if (current != null && current.getKey().compareTo(last) > 0) {
                current = null;
            }
            if (current != null && current.getValue().isDeleted()) //过滤掉已删除的行
                return next();

            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }

    }
}

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
package org.lealone.wiredtiger.dbobject.index;

import org.lealone.api.ErrorCode;
import org.lealone.dbobject.index.IndexBase;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.engine.Constants;
import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.result.SortOrder;
import org.lealone.value.Value;
import org.lealone.value.ValueInt;
import org.lealone.value.ValueLong;
import org.lealone.value.ValueNull;
import org.lealone.value.ValueString;
import org.lealone.wiredtiger.dbobject.table.WiredTigerTable;

public class WiredTigerPrimaryIndex extends IndexBase {
    private final WiredTigerTable table;
    private long lastKey;
    private int mainIndexColumn = -1;

    private final char[] valueFormat;
    private final int length;
    private final com.wiredtiger.db.Cursor wtCursor;

    public WiredTigerPrimaryIndex(Database db, WiredTigerTable table, int id, //
            IndexColumn[] columns, IndexType indexType, com.wiredtiger.db.Session wtSession) {
        this.table = table;
        initIndexBase(table, id, table.getName() + "_DATA", columns, indexType);
        int[] sortTypes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }

        wtCursor = wtSession.open_cursor("table:" + table.getName(), null, "append");

        valueFormat = table.getValueFormat().toCharArray();
        length = valueFormat.length;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public String getPlanSQL() {
        return table.getSQL() + ".tableScan";
    }

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        if (mainIndexColumn == -1) {
            if (row.getKey() == 0) {
                row.setKey(++lastKey);
            }
        } else {
            long c = row.getValue(mainIndexColumn).getLong();
            row.setKey(c);
        }

        if (table.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                Value v2 = v.link(database, getId());
                if (v2.isLinked()) {
                    session.unlinkAtCommitStop(v2);
                }
                if (v != v2) {
                    row.setValue(i, v2);
                }
            }
        }

        wtCursor.putKeyLong(row.getKey());
        putValues(row);
        wtCursor.insert();
        //TODO 
        //        Row old = null;
        //        if (old != null) {
        //            String sql = "PRIMARY KEY ON " + table.getSQL();
        //            if (mainIndexColumn >= 0 && mainIndexColumn < indexColumns.length) {
        //                sql += "(" + indexColumns[mainIndexColumn].getSQL() + ")";
        //            }
        //            DbException e = DbException.get(ErrorCode.DUPLICATE_KEY_1, sql);
        //            e.setSource(this);
        //            throw e;
        //        }
        lastKey = Math.max(lastKey, row.getKey());
    }

    private void putValues(Row row) {
        for (Value v : row.getValueList()) {
            switch (v.getType()) {
            case Value.INT:
                wtCursor.putValueInt(v.getInt());
                break;
            case Value.LONG:
                wtCursor.putValueLong(v.getLong());
                break;
            case Value.STRING:
                wtCursor.putValueString(v.getString());
                break;
            default:
                wtCursor.putValueByteArray(v.getBytesNoCopy());
            }
        }
    }

    @Override
    public void remove(Session session, Row row) {
        if (table.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v.isLinked()) {
                    session.unlinkAtCommit(v);
                }
            }
        }
        wtCursor.putKeyLong(row.getKey());
        wtCursor.remove();
        //TODO
        //        Row old = null;
        //        if (old == null) {
        //            throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, getSQL() + ": " + row.getKey());
        //        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        Value min, max;
        if (first == null) {
            min = null;
        } else if (mainIndexColumn < 0) {
            min = ValueLong.get(first.getKey());
        } else {
            Value v = first.getValue(mainIndexColumn);
            if (v == null) {
                min = ValueLong.get(first.getKey());
            } else {
                min = v;
            }
        }
        if (last == null) {
            max = null;
        } else if (mainIndexColumn < 0) {
            max = ValueLong.get(last.getKey());
        } else {
            Value v = last.getValue(mainIndexColumn);
            if (v == null) {
                max = ValueLong.get(last.getKey());
            } else {
                max = v;
            }
        }
        return find(session, min, max);
    }

    @Override
    public WiredTigerTable getTable() {
        return table;
    }

    @Override
    public Row getRow(Session session, long key) {
        wtCursor.reset();
        wtCursor.putKeyLong(key);
        if (wtCursor.search() != 0) {
            return createNewRow(key);
        }
        return null;
    }

    private Row createNewRow(long key) {
        Value[] values = new Value[length];
        for (int i = 0; i < length; i++) {
            switch (valueFormat[i]) {
            case 'i':
                values[i] = ValueInt.get(wtCursor.getValueInt());
                break;
            case 'q':
                values[i] = ValueLong.get(wtCursor.getValueLong());
                break;
            case 'S':
                values[i] = ValueString.get(wtCursor.getValueString());
                break;
            default:
                values[i] = ValueString.get(wtCursor.getValueString());
                break;
            }
        }
        Row row = new Row(values, 0);
        row.setKey(key);
        return row;
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        try {
            long cost = 10 * (table.getRowCountApproximation() + Constants.COST_ROW_OFFSET);
            return cost;
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public int getColumnIndex(Column col) {
        // can not use this index - use the delegate index instead
        return -1;
    }

    @Override
    public void remove(Session session) {
        //TODO
    }

    @Override
    public void truncate(Session session) {
        if (table.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        return new FirstOrLastRowCursor(first);
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        return getRowCountMax();
    }

    /**
     * The maximum number of rows, including uncommitted rows of any session.
     *
     * @return the maximum number of rows
     */
    public long getRowCountMax() {
        try {
            //TODO
            return 0;
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCountApproximation() {
        return getRowCountMax();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * Get the key from the row.
     *
     * @param row the row
     * @param ifEmpty the value to use if the row is empty
     * @param ifNull the value to use if the column is NULL
     * @return the key
     */
    ValueLong getKey(SearchRow row, ValueLong ifEmpty, ValueLong ifNull) {
        if (row == null) {
            return ifEmpty;
        }
        Value v = row.getValue(mainIndexColumn);
        if (v == null) {
            throw DbException.throwInternalError(row.toString());
        } else if (v == ValueNull.INSTANCE) {
            return ifNull;
        }
        return (ValueLong) v.convertTo(Value.LONG);
    }

    /**
     * Search for a specific row or a set of rows.
     *
     * @param session the session
     * @param first the key of the first row
     * @param last the key of the last row
     * @return the cursor
     */
    Cursor find(Session session, Value first, Value last) {
        return new WiredTigerPrimaryIndexCursor(first, last);
    }

    @Override
    public boolean isRowIdIndex() {
        return true;
    }

    private class FirstOrLastRowCursor implements Cursor {
        private Row row = null;
        private boolean end;

        public FirstOrLastRowCursor(boolean first) {
            wtCursor.reset();
            if (first) {
                if (wtCursor.next() != 0) {
                    row = createNewRow(wtCursor.getKeyLong());
                }
            } else {
                if (wtCursor.prev() != 0) {
                    row = createNewRow(wtCursor.getKeyLong());
                }
            }
        }

        @Override
        public Row get() {
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            if (row != null && !end) {
                end = true;
                return true;
            }
            return false;
        }

        @Override
        public boolean previous() {
            return false;
        }
    }

    private class WiredTigerPrimaryIndexCursor implements Cursor {
        private final com.wiredtiger.db.Cursor wtCursor;
        private final Value last;
        private Row row;
        private boolean searched;

        public WiredTigerPrimaryIndexCursor(Value first, Value last) {
            wtCursor = WiredTigerPrimaryIndex.this.wtCursor;
            wtCursor.reset();
            if (first != null)
                wtCursor.putKeyLong(first.getLong());
            else
                searched = true;
            this.last = last;
        }

        @Override
        public Row get() {
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            if (!searched) {
                searched = true;
                if (wtCursor.search() != 0)
                    return false;
                row = createNewRow(wtCursor.getKeyLong());
                return true;
            }

            if (wtCursor.next() != 0)
                return false;

            long key = wtCursor.getKeyLong();
            if (last != null && key > last.getLong())
                return false;

            row = createNewRow(key);
            return true;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }
}

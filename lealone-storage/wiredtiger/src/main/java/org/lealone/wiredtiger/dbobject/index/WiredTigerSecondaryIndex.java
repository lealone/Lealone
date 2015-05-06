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
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.result.SortOrder;
import org.lealone.value.Value;
import org.lealone.value.ValueInt;
import org.lealone.value.ValueLong;
import org.lealone.value.ValueString;
import org.lealone.wiredtiger.dbobject.table.WiredTigerTable;

public class WiredTigerSecondaryIndex extends IndexBase {
    private final com.wiredtiger.db.Cursor wtCursor;
    private final char[] indexColumnFormat;
    private final int indexColumnLength;
    private final WiredTigerTable table;

    public WiredTigerSecondaryIndex(WiredTigerTable table, int id, String indexName, //
            IndexColumn[] columns, IndexType indexType, com.wiredtiger.db.Session wtSession) {
        this.table = table;
        initIndexBase(table, id, indexName, columns, indexType);
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }

        StringBuilder columnNames = new StringBuilder();
        StringBuilder indexColumnFormat = new StringBuilder();
        for (IndexColumn c : columns) {
            if (columnNames.length() != 0)
                columnNames.append(',');
            columnNames.append(c.columnName);

            indexColumnFormat.append(WiredTigerTable.getWiredTigerType(c.column.getType()));
        }
        this.indexColumnFormat = indexColumnFormat.toString().toCharArray();
        this.indexColumnLength = this.indexColumnFormat.length;
        wtSession.create("index:" + table.getName() + ":" + indexName, "columns=(" + columnNames + ")");
        String uri = "index:" + table.getName() + ":" + indexName + "(" + Column.ROWID + ")";
        wtCursor = wtSession.open_cursor(uri, null, null);
    }

    @Override
    public void close(Session session) {
    }

    @Override
    public void add(Session session, Row row) {
    }

    @Override
    public void remove(Session session, Row row) {
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return new WiredTigerSecondaryIndexCursor(first, last);
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        try {
            return 10 * getCostRangeIndex(masks, getRowCountApproximation(), sortOrder);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public void remove(Session session) {
    }

    @Override
    public void truncate(Session session) {
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
        return getRowCountApproximation();
    }

    @Override
    public long getRowCountApproximation() {
        //TODO
        return table.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public void checkRename() {
        //ok
    }

    private SearchRow createNewRow() {
        SearchRow searchRow = table.getTemplateRow();
        Column[] cols = getColumns();
        Value v;
        for (int i = 0; i < indexColumnLength; i++) {
            switch (indexColumnFormat[i]) {
            case 'i':
                v = ValueInt.get(wtCursor.getKeyInt());
                break;
            case 'q':
                v = ValueLong.get(wtCursor.getKeyLong());
                break;
            case 'S':
                v = ValueString.get(wtCursor.getKeyString());
                break;
            default:
                v = ValueString.get(wtCursor.getKeyString());
                break;
            }

            int idx = cols[i].getColumnId();
            searchRow.setValue(idx, v);
        }
        searchRow.setKey(wtCursor.getValueLong()); //对应Column.ROWID
        return searchRow;
    }

    private class FirstOrLastRowCursor implements Cursor {
        private Row row;
        private SearchRow searchRow;
        private boolean end;

        public FirstOrLastRowCursor(boolean first) {
            wtCursor.reset();
            if (first) {
                if (wtCursor.next() != 0) {
                    searchRow = createNewRow();
                }
            } else {
                if (wtCursor.prev() != 0) {
                    searchRow = createNewRow();
                }
            }
        }

        @Override
        public Row get() {
            if (searchRow != null)
                row = table.getRow(null, wtCursor.getKeyLong());
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return searchRow;
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

    private void putKeys(SearchRow r) {
        if (r == null) {
            return;
        }
        Value[] array = new Value[columns.length];
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            int idx = c.getColumnId();
            Value v = r.getValue(idx);
            if (v != null) {
                array[i] = v.convertTo(c.getType());
            }
        }

        for (Value v : array) {
            switch (v.getType()) {
            case Value.INT:
                wtCursor.putKeyInt(v.getInt());
                break;
            case Value.LONG:
                wtCursor.putKeyLong(v.getLong());
                break;
            case Value.STRING:
                wtCursor.putKeyString(v.getString());
                break;
            default:
                wtCursor.putKeyByteArray(v.getBytesNoCopy());
            }
        }
    }

    private class WiredTigerSecondaryIndexCursor implements Cursor {
        private final com.wiredtiger.db.Cursor wtCursor;
        private final SearchRow last;
        private Row row;
        private SearchRow searchRow;
        private boolean searched;

        public WiredTigerSecondaryIndexCursor(SearchRow first, SearchRow last) {
            wtCursor = WiredTigerSecondaryIndex.this.wtCursor;
            wtCursor.reset();
            if (first != null)
                putKeys(first);
            else
                searched = true;
            this.last = last;
        }

        @Override
        public Row get() {
            if (searchRow != null)
                row = table.getRow(null, wtCursor.getKeyLong());
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return searchRow;
        }

        @Override
        public boolean next() {
            if (!searched) {
                searched = true;
                if (wtCursor.search() != 0)
                    return false;
                searchRow = createNewRow();
                return true;
            }

            if (wtCursor.next() != 0)
                return false;

            searchRow = createNewRow();
            if (last != null && compareRows(searchRow, last) > 0) {
                searchRow = null;
                return false;
            }
            return true;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }
}

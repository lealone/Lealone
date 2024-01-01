/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.optimizer;

import java.util.ArrayList;
import java.util.HashSet;

import com.lealone.db.index.Cursor;
import com.lealone.db.index.Index;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.result.Result;
import com.lealone.db.result.Row;
import com.lealone.db.result.SearchRow;
import com.lealone.db.result.SortOrder;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.expression.condition.Comparison;
import com.lealone.sql.query.Select;
import com.lealone.storage.CursorParameters;

/**
 * The filter used to walk through an index. This class supports IN(..)
 * and IN(SELECT ...) optimizations.
 */
public class IndexCursor implements Cursor {

    private final TableFilter tableFilter;
    private Table table;
    private Index index;
    private IndexColumn[] indexColumns;
    private boolean alwaysFalse;

    private SearchRow start, end;
    private Cursor cursor;

    private Column inColumn;
    private int inListIndex;
    private Value[] inList;
    private Result inResult;
    private HashSet<Value> inResultTested;

    public IndexCursor(TableFilter filter) {
        this.tableFilter = filter;
    }

    public void setIndex(Index index) {
        this.index = index;
        this.table = index.getTable();
        Column[] columns = table.getColumns();
        indexColumns = new IndexColumn[columns.length];
        IndexColumn[] idxCols = index.getIndexColumns();
        if (idxCols != null) {
            for (int i = 0, len = columns.length; i < len; i++) {
                int idx = index.getColumnIndex(columns[i]);
                if (idx >= 0) {
                    indexColumns[i] = idxCols[idx];
                }
            }
        }
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    /**
     * Re-evaluate the start and end values of the index search for rows.
     *
     * @param session the session
     * @param indexConditions the index conditions
     */
    public void find(ServerSession session, ArrayList<IndexCondition> indexConditions) {
        parseIndexConditions(session, indexConditions);
        if (inColumn != null) {
            return;
        }
        if (!alwaysFalse) {
            Select select = tableFilter.getSelect();
            int[] columnIndexes = null;
            if (select != null) {
                columnIndexes = tableFilter.createColumnIndexes(select.getReferencedColumns());
            } else {
                columnIndexes = tableFilter.getColumnIndexes(); // update和delete在prepare阶段就设置好了
            }
            CursorParameters<SearchRow> parameters = CursorParameters.create(start, end, columnIndexes);
            cursor = index.find(tableFilter.getSession(), parameters);
        }
    }

    public void parseIndexConditions(ServerSession session, ArrayList<IndexCondition> indexConditions) {
        alwaysFalse = false;
        start = end = null;
        inList = null;
        inColumn = null;
        inResult = null;
        inResultTested = null;
        // don't use enhanced for loop to avoid creating objects
        for (int i = 0, size = indexConditions.size(); i < size; i++) {
            IndexCondition condition = indexConditions.get(i);
            if (condition.isAlwaysFalse()) {
                alwaysFalse = true;
                break;
            }
            Column column = condition.getColumn();
            if (condition.getCompareType() == Comparison.IN_LIST) {
                if (start == null && end == null) {
                    if (canUseIndexForIn(column)) {
                        this.inColumn = column;
                        inList = condition.getCurrentValueList(session);
                        inListIndex = 0;
                    }
                }
            } else if (condition.getCompareType() == Comparison.IN_QUERY) {
                if (start == null && end == null) {
                    if (canUseIndexForIn(column)) {
                        this.inColumn = column;
                        inResult = condition.getCurrentResult();
                    }
                }
            } else {
                Value v = condition.getCurrentValue(session);
                boolean isStart = condition.isStart();
                boolean isEnd = condition.isEnd();
                int id = column.getColumnId();
                if (id >= 0) {
                    IndexColumn idxCol = indexColumns[id];
                    if (idxCol != null && (idxCol.sortType & SortOrder.DESCENDING) != 0) {
                        // if the index column is sorted the other way, we swap end and start
                        // NULLS_FIRST / NULLS_LAST is not a problem, as nulls never match anyway
                        boolean temp = isStart;
                        isStart = isEnd;
                        isEnd = temp;
                    }
                }
                if (isStart) {
                    start = getSearchRow(session, start, id, v, true);
                }
                if (isEnd) {
                    end = getSearchRow(session, end, id, v, false);
                }
                if (isStart || isEnd) {
                    // an X=? condition will produce less rows than
                    // an X IN(..) condition
                    inColumn = null;
                    inList = null;
                    inResult = null;
                }
                if (!session.getDatabase().getSettings().optimizeIsNull) {
                    if (isStart && isEnd) {
                        if (v == ValueNull.INSTANCE) {
                            // join on a column=NULL is always false
                            alwaysFalse = true;
                        }
                    }
                }
            }
        }
    }

    private boolean canUseIndexForIn(Column column) {
        if (inColumn != null) {
            // only one IN(..) condition can be used at the same time
            return false;
        }
        // The first column of the index must match this column,
        // or it must be a VIEW index (where the column is null).
        // Multiple IN conditions with views are not supported, see
        // IndexCondition.getMask.
        IndexColumn[] cols = index.getIndexColumns();
        if (cols == null) {
            return true;
        }
        IndexColumn idxCol = cols[0];
        return idxCol == null || idxCol.column == column;
    }

    private SearchRow getSearchRow(ServerSession session, SearchRow row, int id, Value v, boolean max) {
        if (row == null) {
            row = table.getTemplateRow();
        } else {
            v = getMax(session, row.getValue(id), v, max);
        }
        if (id < 0) {
            row.setKey(v.getLong());
        } else {
            row.setValue(id, v);
        }
        return row;
    }

    private Value getMax(ServerSession session, Value a, Value b, boolean bigger) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        }
        if (session.getDatabase().getSettings().optimizeIsNull) {
            // IS NULL must be checked later
            if (a == ValueNull.INSTANCE) {
                return b;
            } else if (b == ValueNull.INSTANCE) {
                return a;
            }
        }
        int comp = a.compareTo(b, table.getDatabase().getCompareMode());
        if (comp == 0) {
            return a;
        }
        if (a == ValueNull.INSTANCE || b == ValueNull.INSTANCE) {
            if (session.getDatabase().getSettings().optimizeIsNull) {
                // column IS NULL AND column <op> <not null> is always false
                return null;
            }
        }
        if (!bigger) {
            comp = -comp;
        }
        return comp > 0 ? a : b;
    }

    @Override
    public Row get() {
        if (cursor == null)
            return null;
        return cursor.get();
    }

    @Override
    public Row get(int[] columnIndexes) {
        if (cursor == null)
            return null;
        return cursor.get(columnIndexes);
    }

    @Override
    public SearchRow getSearchRow() {
        if (cursor == null)
            return null;
        return cursor.getSearchRow();
    }

    @Override
    public boolean next() {
        while (true) {
            if (cursor == null) {
                nextCursor();
                if (cursor == null) {
                    return false;
                }
            }
            if (cursor.next()) {
                return true;
            }
            cursor = null;
        }
    }

    private void nextCursor() {
        if (inList != null) {
            while (inListIndex < inList.length) {
                Value v = inList[inListIndex++];
                if (v != ValueNull.INSTANCE) {
                    v = inColumn.convert(v);
                    find(v);
                    break;
                }
            }
        } else if (inResult != null) {
            while (inResult.next()) {
                Value v = inResult.currentRow()[0];
                if (v != ValueNull.INSTANCE) {
                    v = inColumn.convert(v);
                    if (inResultTested == null) {
                        inResultTested = new HashSet<>();
                    }
                    if (inResultTested.add(v)) {
                        find(v);
                        break;
                    }
                }
            }
        }
    }

    private void find(Value v) {
        int id = inColumn.getColumnId();
        if (start == null) {
            start = table.getTemplateRow();
        }
        start.setValue(id, v);
        cursor = index.find(tableFilter.getSession(), start, start);
    }
}

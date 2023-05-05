/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import org.lealone.db.index.Cursor;
import org.lealone.db.index.Index;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.value.Value;

// 单字段/多字段distinct
class QDistinct extends QOperator {

    private Index index;
    private int[] columnIds;
    private int size;
    private Cursor cursor;

    QDistinct(Select select) {
        super(select);
    }

    @Override
    protected Row getRow() {
        SearchRow found = cursor.getSearchRow();
        return topTableFilter.getTable().getRow(session, found.getKey());
    }

    @Override
    protected boolean next() {
        return cursor.next();
    }

    @Override
    public void start() {
        index = topTableFilter.getIndex();
        columnIds = index.getColumnIds();
        size = columnIds.length;
        cursor = index.findDistinct(session);
        yieldableSelect.disableOlap(); // 无需从oltp转到olap
        super.start();
    }

    @Override
    public void run() {
        rebuildSearchRowIfNeeded();
        while (hasNext) {
            if (select.isForUpdate && !tryLockRow()) {
                return; // 锁记录失败
            }
            boolean yield = yieldIfNeeded(++loopCount);
            SearchRow found = cursor.getSearchRow();
            Value[] row = new Value[size];
            for (int i = 0; i < size; i++) {
                row[i] = found.getValue(columnIds[i]);
            }
            result.addRow(row);
            rowCount++;
            if (canBreakLoop()) {
                break;
            }
            if (yield)
                return;
            hasNext = next();
        }
        loopEnd = true;
    }
}

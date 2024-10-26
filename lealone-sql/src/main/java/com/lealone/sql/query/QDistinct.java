/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.query;

import com.lealone.db.index.Cursor;
import com.lealone.db.index.Index;
import com.lealone.db.row.SearchRow;
import com.lealone.db.value.Value;

// 单字段/多字段distinct
class QDistinct extends QOperator {

    private final Index index;
    private final int[] columnIds;
    private final int size;
    private Cursor cursor;

    QDistinct(Select select) {
        super(select);
        index = select.getTopTableFilter().getIndex();
        columnIds = index.getColumnIds();
        size = columnIds.length;
    }

    @Override
    public void start() {
        cursor = index.findDistinct(session);
        yieldableSelect.disableOlap(); // 无需从oltp转到olap
        tableIterator.setCursor(cursor);
        super.start();
    }

    @Override
    public void run() {
        while (next()) {
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
        }
        loopEnd = true;
    }
}

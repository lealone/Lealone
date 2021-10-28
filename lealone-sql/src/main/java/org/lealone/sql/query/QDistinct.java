/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import org.lealone.db.index.Cursor;
import org.lealone.db.index.Index;
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
    public void start() {
        super.start();
        index = select.topTableFilter.getIndex();
        columnIds = index.getColumnIds();
        size = columnIds.length;
        cursor = index.findDistinct(session, null, null);
        yieldableSelect.disableOlap(); // 无需从oltp转到olap
    }

    @Override
    public void run() {
        while (cursor.next()) {
            if (select.isForUpdate && !select.topTableFilter.lockRow())
                return; // 锁记录失败
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

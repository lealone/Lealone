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

// 单字段distinct
class QDistinct extends QOperator {

    private Index index;
    private int columnIndex;
    private Cursor cursor;

    QDistinct(Select select) {
        super(select);
    }

    @Override
    void start() {
        super.start();
        index = select.topTableFilter.getIndex();
        columnIndex = index.getColumns()[0].getColumnId();
        cursor = index.findDistinct(session, null, null);
    }

    @Override
    void run() {
        while (cursor.next()) {
            if (select.isForUpdate && !select.topTableFilter.lockRow())
                return; // 锁记录失败
            ++loopCount;
            SearchRow found = cursor.getSearchRow();
            Value value = found.getValue(columnIndex);
            Value[] row = { value };
            result.addRow(row);
            rowCount++;
            if (canBreakLoop()) {
                break;
            }
            if (yieldIfNeeded(loopCount))
                return;
        }
        loopEnd = true;
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.query;

import com.lealone.db.value.Value;

// 对min、max、count三个聚合函数的特殊优化
class QAggregateQuick extends QOperator {

    QAggregateQuick(Select select) {
        super(select);
    }

    @Override
    public void start() {
        // 什么都不需要做
    }

    @Override
    public void run() {
        Value[] row = createRow();
        result.addRow(row);
        rowCount = 1;
        loopEnd = true;
    }
}

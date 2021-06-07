/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;

// 对min、max、count三个聚合函数的特殊优化
class QQuick extends QOperator {

    QQuick(Select select) {
        super(select);
    }

    @Override
    void start() {
        // 什么都不需要做
    }

    @Override
    void run() {
        Value[] row = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Expression expr = select.expressions.get(i);
            row[i] = expr.getValue(session);
        }
        result.addRow(row);
        rowCount = 1;
        loopEnd = true;
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.HashMap;

import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;

// 除了QuickAggregateQuery之外的聚合函数，没有group by
class QAggregate extends QOperator {

    QAggregate(Select select) {
        super(select);
    }

    @Override
    public void start() {
        super.start();
        select.currentGroup = new HashMap<Expression, Object>();
    }

    @Override
    public void run() {
        while (select.topTableFilter.next()) {
            boolean yield = yieldIfNeeded(++loopCount);
            if (conditionEvaluator.getBooleanValue()) {
                if (select.isForUpdate && !select.topTableFilter.lockRow())
                    return; // 锁记录失败
                rowCount++;
                select.currentGroupRowId++;
                for (int i = 0; i < columnCount; i++) {
                    Expression expr = select.expressions.get(i);
                    expr.updateAggregate(session);
                }
                if (sampleSize > 0 && rowCount >= sampleSize) {
                    break;
                }
            }
            if (yield)
                return;
        }
        Value[] row = createRow();
        row = QGroup.toResultRow(row, columnCount, select.resultColumnCount);
        result.addRow(row);
        loopEnd = true;
    }
}

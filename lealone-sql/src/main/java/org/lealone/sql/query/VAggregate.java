/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.HashMap;

import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.vector.ValueVector;

// 除了QuickAggregateQuery之外的聚合函数，没有group by
class VAggregate extends VOperator {

    VAggregate(Select select) {
        super(select);
    }

    @Override
    public void start() {
        super.start();
        select.currentGroup = new HashMap<Expression, Object>();
    }

    @Override
    public void run() {
        while (select.topTableFilter.nextBatch()) {
            ++loopCount;

            ValueVector conditionValueVector = null;
            if (select.condition != null) {
                conditionValueVector = select.condition.getValueVector(session);
            }
            rowCount++;
            select.currentGroupRowId++;
            for (int i = 0; i < columnCount; i++) {
                Expression expr = select.expressions.get(i);
                expr.updateVectorizedAggregate(session, conditionValueVector);
            }
            if (sampleSize > 0 && rowCount >= sampleSize) {
                break;
            }
            if (yieldIfNeeded(loopCount))
                return;
        }
        Value[] row = createRow();
        row = QGroup.keepOnlyDistinct(row, columnCount, select.distinctColumnCount);
        result.addRow(row);
        loopEnd = true;
    }
}

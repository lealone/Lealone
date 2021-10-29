/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.HashMap;

import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.visitor.UpdateVectorizedAggregateVisitor;
import org.lealone.sql.vector.ValueVector;

// 除了QuickAggregateQuery之外的聚合函数，没有group by
class VAggregate extends VOperator {

    VAggregate(Select select) {
        super(select);
        if (select.currentGroup == null)
            select.currentGroup = new HashMap<>();
    }

    @Override
    public void run() {
        while (nextBatch()) {
            boolean yield = yieldIfNeeded(++loopCount);
            select.topTableFilter.setBatchSize(batch.size());
            ValueVector conditionValueVector = getConditionValueVector();
            UpdateVectorizedAggregateVisitor visitor = new UpdateVectorizedAggregateVisitor(select.topTableFilter,
                    session, conditionValueVector, batch);
            select.currentGroupRowId++;
            for (int i = 0; i < columnCount; i++) {
                Expression expr = select.expressions.get(i);
                expr.accept(visitor);
            }
            rowCount += getBatchSize(conditionValueVector);
            if (sampleSize > 0 && rowCount >= sampleSize) {
                break;
            }
            if (yield)
                return;
        }
        // 最后把聚合后的结果增加到结果集中
        Value[] row = createRow();
        row = QGroup.toResultRow(row, columnCount, select.resultColumnCount);
        result.addRow(row);
        loopEnd = true;
    }
}

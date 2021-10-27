/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.HashMap;

import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.visitor.GetValueVectorVisitor;
import org.lealone.sql.expression.visitor.UpdateVectorizedAggregateVisitor;
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
        while (nextBatch()) {
            select.topTableFilter.setBatchSize(batch.size());
            boolean yield = yieldIfNeeded(++loopCount);

            ValueVector conditionValueVector = null;
            if (select.condition != null) {
                GetValueVectorVisitor v = new GetValueVectorVisitor(session, null, batch);
                conditionValueVector = select.condition.accept(v);
            }
            rowCount++;
            select.currentGroupRowId++;
            for (int i = 0; i < columnCount; i++) {
                Expression expr = select.expressions.get(i);
                expr.accept(new UpdateVectorizedAggregateVisitor(session, conditionValueVector, batch));
            }
            if (sampleSize > 0 && rowCount >= sampleSize) {
                break;
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

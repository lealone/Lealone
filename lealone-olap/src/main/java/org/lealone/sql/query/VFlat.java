/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.visitor.GetValueVectorVisitor;
import org.lealone.sql.vector.ValueVector;

// 最普通的查询
class VFlat extends VOperator {

    VFlat(Select select) {
        super(select);
    }

    @Override
    public void run() {
        while (nextBatch()) {
            boolean yield = yieldIfNeeded(++loopCount);
            ValueVector conditionValueVector = getConditionValueVector();
            GetValueVectorVisitor visitor = new GetValueVectorVisitor(select.topTableFilter, session,
                    conditionValueVector, batch);
            ValueVector[] rows = new ValueVector[columnCount];
            for (int i = 0; i < columnCount; i++) {
                Expression expr = select.expressions.get(i);
                rows[i] = expr.accept(visitor);
            }
            for (int i = 0, szie = rows[0].size(); i < szie; i++) {
                Value[] row = new Value[columnCount];
                for (int j = 0; j < columnCount; j++) {
                    ValueVector vv = rows[j];
                    row[j] = vv.getValue(i);
                }
                result.addRow(row);
            }
            rowCount += getBatchSize(conditionValueVector);
            if (canBreakLoop()) {
                break;
            }
            if (yield)
                return;
        }
        loopEnd = true;
    }
}

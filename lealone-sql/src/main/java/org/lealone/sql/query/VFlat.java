/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.vector.ValueVector;

// 最普通的查询
class VFlat extends VOperator {

    VFlat(Select select) {
        super(select);
    }

    @Override
    public void run() {
        while (select.topTableFilter.nextBatch()) {
            ++loopCount;

            ValueVector conditionValueVector = null;
            if (select.condition != null) {
                conditionValueVector = select.condition.getValueVector(session);
            }
            ValueVector[] rows = new ValueVector[columnCount];
            for (int i = 0; i < columnCount; i++) {
                Expression expr = select.expressions.get(i);
                rows[i] = expr.getValueVector(session);
            }
            if (conditionValueVector != null) {
                for (int i = 0, szie = conditionValueVector.size(); i < szie; i++) {
                    if (conditionValueVector.isTrue(i)) {
                        Value[] row = new Value[columnCount];
                        for (int j = 0; j < columnCount; j++) {
                            ValueVector vv = rows[j];
                            row[j] = vv.getValue(i);
                        }
                        result.addRow(row);
                    }
                }
            }
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

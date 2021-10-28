/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.Arrays;
import java.util.HashMap;

import org.lealone.db.result.ResultTarget;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;

// 只处理group by，且group by的字段有对应的索引
class QGroupSorted extends QOperator {

    private Value[] previousKeyValues;

    QGroupSorted(Select select) {
        super(select);
    }

    @Override
    public void start() {
        super.start();
        select.currentGroup = null;
    }

    @Override
    public void run() {
        while (select.topTableFilter.next()) {
            boolean yield = yieldIfNeeded(++loopCount);
            if (conditionEvaluator.getBooleanValue()) {
                if (select.isForUpdate && !select.topTableFilter.lockRow())
                    return; // 锁记录失败
                rowCount++;
                Value[] keyValues = new Value[select.groupIndex.length];
                // update group
                for (int i = 0; i < select.groupIndex.length; i++) {
                    int idx = select.groupIndex[i];
                    Expression expr = select.expressions.get(idx);
                    keyValues[i] = expr.getValue(session);
                }

                if (previousKeyValues == null) {
                    previousKeyValues = keyValues;
                    select.currentGroup = new HashMap<>();
                } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    addGroupSortedRow(previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    select.currentGroup = new HashMap<>();
                }
                select.currentGroupRowId++;

                for (int i = 0; i < columnCount; i++) {
                    if (!select.groupByExpression[i]) {
                        Expression expr = select.expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
                if (yield)
                    return;
            }
        }
        if (previousKeyValues != null) {
            addGroupSortedRow(previousKeyValues, columnCount, result);
        }
        loopEnd = true;
    }

    private void addGroupSortedRow(Value[] keyValues, int columnCount, ResultTarget result) {
        QGroup.addGroupRow(select, keyValues, columnCount, result);
    }
}

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

class QGroupSorted extends QOperator {

    private Value[] previousKeyValues;

    QGroupSorted(Select select) {
        super(select);
    }

    @Override
    void start() {
        super.start();
        select.currentGroup = null;
    }

    @Override
    void run() {
        while (select.topTableFilter.next()) {
            ++loopCount;
            if (select.condition == null || select.condition.getBooleanValue(session)) {
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
                if (yieldIfNeeded(loopCount))
                    return;
            }
        }
        if (previousKeyValues != null) {
            addGroupSortedRow(previousKeyValues, columnCount, result);
        }
        loopEnd = true;
    }

    private void addGroupSortedRow(Value[] keyValues, int columnCount, ResultTarget result) {
        Value[] row = new Value[columnCount];
        for (int i = 0; i < select.groupIndex.length; i++) {
            row[select.groupIndex[i]] = keyValues[i];
        }
        for (int i = 0; i < columnCount; i++) {
            if (select.groupByExpression[i]) {
                continue;
            }
            Expression expr = select.expressions.get(i);
            row[i] = expr.getValue(session);
        }
        if (QGroup.isHavingNullOrFalse(row, select.havingIndex)) {
            return;
        }
        row = QGroup.keepOnlyDistinct(row, columnCount, select.distinctColumnCount);
        result.addRow(row);
    }
}

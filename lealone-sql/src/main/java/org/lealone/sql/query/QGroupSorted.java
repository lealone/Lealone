/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.Arrays;
import java.util.HashMap;

import org.lealone.db.value.Value;

// 只处理group by，且group by的字段有对应的索引
class QGroupSorted extends QOperator {

    private Value[] previousKeyValues;

    QGroupSorted(Select select) {
        super(select);
        select.currentGroup = null;
    }

    public Value[] getPreviousKeyValues() {
        return previousKeyValues;
    }

    @Override
    public void run() {
        while (next()) {
            boolean yield = yieldIfNeeded(++loopCount);
            if (conditionEvaluator.getBooleanValue()) {
                if (select.isForUpdate && !tryLockRow()) {
                    return; // 锁记录失败
                }
                rowCount++;
                Value[] keyValues = QGroup.getKeyValues(select);
                if (previousKeyValues == null) {
                    previousKeyValues = keyValues;
                    select.currentGroup = new HashMap<>();
                } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    QGroup.addGroupRow(select, previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    select.currentGroup = new HashMap<>();
                }
                select.currentGroupRowId++;
                QGroup.updateAggregate(select, columnCount);
                if (yield)
                    return;
            }
        }
        if (previousKeyValues != null) {
            QGroup.addGroupRow(select, previousKeyValues, columnCount, result);
        }
        loopEnd = true;
    }
}

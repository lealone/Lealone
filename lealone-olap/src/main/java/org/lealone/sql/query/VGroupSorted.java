/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.lealone.db.value.Value;

// 只处理group by，且group by的字段有对应的索引
class VGroupSorted extends VOperator {

    private Value[] previousKeyValues;

    VGroupSorted(Select select) {
        super(select);
        select.currentGroup = null;
    }

    @Override
    public void run() {
        batch = new ArrayList<>();
        while (select.topTableFilter.next()) {
            boolean yield = yieldIfNeeded(++loopCount);
            if (conditionEvaluator.getBooleanValue()) {
                if (select.isForUpdate && !select.topTableFilter.lockRow())
                    return; // 锁记录失败
                rowCount++;
                Value[] keyValues = QGroup.getKeyValues(select);
                if (previousKeyValues == null) {
                    previousKeyValues = keyValues;
                    select.currentGroup = new HashMap<>();
                } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    VGroup.updateVectorizedAggregate(select, columnCount, batch);
                    QGroup.addGroupRow(select, previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    select.currentGroup = new HashMap<>();
                }
                batch.add(select.topTableFilter.get());
                if (yield)
                    return;
            }
        }
        if (previousKeyValues != null && !batch.isEmpty()) {
            VGroup.updateVectorizedAggregate(select, columnCount, batch);
            QGroup.addGroupRow(select, previousKeyValues, columnCount, result);
        }
        loopEnd = true;
    }
}

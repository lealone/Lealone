/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.ArrayList;
import java.util.HashMap;

import org.lealone.db.result.Row;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.visitor.UpdateVectorizedAggregateVisitor;
import org.lealone.sql.operator.Operator;

// 只处理group by，且group by的字段没有索引
class VGroup extends VOperator {

    private ValueHashMap<HashMap<Expression, Object>> groups;
    private ValueHashMap<ArrayList<Row>> batchMap;

    VGroup(Select select) {
        super(select);
        select.currentGroup = null;
        groups = ValueHashMap.newInstance();
        batchMap = ValueHashMap.newInstance();
    }

    @Override
    public void copyStatus(Operator old) {
        super.copyStatus(old);
        if (old instanceof QGroup) {
            QGroup q = (QGroup) old;
            groups = q.getGroups();
        }
    }

    @Override
    public void run() {
        while (select.topTableFilter.next()) {
            boolean yield = yieldIfNeeded(++loopCount);
            if (conditionEvaluator.getBooleanValue()) {
                if (select.isForUpdate && !select.topTableFilter.lockRow())
                    return; // 锁记录失败
                rowCount++;
                Value key = QGroup.getKey(select);
                batch = batchMap.get(key);
                if (batch == null) {
                    batch = new ArrayList<>();
                    batchMap.put(key, batch);
                }
                batch.add(select.topTableFilter.get());
                if (batch.size() >= 1024) {
                    updateVectorizedAggregate(key);
                }
                if (sampleSize > 0 && rowCount >= sampleSize) {
                    break;
                }
            }
            if (yield)
                return;
        }
        for (Value key : batchMap.keys()) {
            batch = batchMap.get(key);
            if (!batch.isEmpty()) {
                updateVectorizedAggregate(key);
            }
        }
        QGroup.addGroupRows(groups, select, columnCount, result);
        loopEnd = true;
    }

    private void updateVectorizedAggregate(Value key) {
        select.currentGroup = QGroup.getOrCreateGroup(groups, key);
        updateVectorizedAggregate(select, columnCount, batch);
    }

    static void updateVectorizedAggregate(Select select, int columnCount, ArrayList<Row> batch) {
        select.currentGroupRowId++;
        UpdateVectorizedAggregateVisitor visitor = new UpdateVectorizedAggregateVisitor(select.getSession(), null,
                batch);
        for (int i = 0; i < columnCount; i++) {
            if (select.groupByExpression == null || !select.groupByExpression[i]) {
                Expression expr = select.expressions.get(i);
                expr.accept(visitor);
            }
        }
        batch.clear();
    }
}

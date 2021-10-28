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
import org.lealone.db.value.ValueArray;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.visitor.UpdateVectorizedAggregateVisitor;
import org.lealone.sql.operator.Operator;

// 只处理group by，且group by的字段没有索引
class VGroup extends VOperator {

    private ValueHashMap<HashMap<Expression, Object>> groups;
    private ValueHashMap<ArrayList<Row>> batchMap;

    VGroup(Select select) {
        super(select);
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
    public void start() {
        super.start();
        groups = ValueHashMap.newInstance();
        batchMap = ValueHashMap.newInstance();
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
                // 避免在ExpressionColumn.getValue中取到旧值
                // 例如SELECT id/3 AS A, COUNT(*) FROM mytable GROUP BY A HAVING A>=0
                select.currentGroup = null;
                Value[] keyValues = new Value[select.groupIndex.length];
                // update group
                for (int i = 0; i < select.groupIndex.length; i++) {
                    int idx = select.groupIndex[i];
                    Expression expr = select.expressions.get(idx);
                    keyValues[i] = expr.getValue(session);
                }
                Value key = ValueArray.get(keyValues);
                HashMap<Expression, Object> values = groups.get(key);
                batch = batchMap.get(key);
                if (values == null) {
                    values = new HashMap<Expression, Object>();
                    groups.put(key, values);
                }
                if (batch == null) {
                    batch = new ArrayList<>();
                    batchMap.put(key, batch);
                }
                batch.add(select.topTableFilter.get());
                if (batch.size() > 512) {
                    updateVectorizedAggregate(values);
                }
                if (sampleSize > 0 && rowCount >= sampleSize) {
                    break;
                }
            }
            if (yield)
                return;
        }

        for (Value v : groups.keys()) {
            batch = batchMap.get(v);
            if (!batch.isEmpty()) {
                HashMap<Expression, Object> values = groups.get(v);
                updateVectorizedAggregate(values);
            }

            ValueArray key = (ValueArray) v;
            select.currentGroup = groups.get(key);
            Value[] keyValues = key.getList();
            QGroup.addGroupRow(select, keyValues, columnCount, result);
        }
        loopEnd = true;
    }

    private void updateVectorizedAggregate(HashMap<Expression, Object> values) {
        select.currentGroup = values;
        select.currentGroupRowId++;
        UpdateVectorizedAggregateVisitor visitor = new UpdateVectorizedAggregateVisitor(session, null, batch);
        for (int i = 0; i < columnCount; i++) {
            if (select.groupByExpression == null || !select.groupByExpression[i]) {
                Expression expr = select.expressions.get(i);
                expr.accept(visitor);
            }
        }
        batch.clear();
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.expression.Expression;

// 只处理group by，且group by的字段没有索引
class QGroup extends QOperator {

    private ValueHashMap<HashMap<Expression, Object>> groups;

    QGroup(Select select) {
        super(select);
    }

    public ValueHashMap<HashMap<Expression, Object>> getGroups() {
        return groups;
    }

    @Override
    public void start() {
        super.start();
        groups = ValueHashMap.newInstance();
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
                if (values == null) {
                    values = new HashMap<Expression, Object>();
                    groups.put(key, values);
                }
                select.currentGroup = values;
                select.currentGroupRowId++;
                for (int i = 0; i < columnCount; i++) {
                    if (select.groupByExpression == null || !select.groupByExpression[i]) {
                        Expression expr = select.expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
                if (sampleSize > 0 && rowCount >= sampleSize) {
                    break;
                }
            }
            if (yield)
                return;
        }
        ArrayList<Value> keys = groups.keys();
        for (Value v : keys) {
            ValueArray key = (ValueArray) v;
            select.currentGroup = groups.get(key);
            Value[] keyValues = key.getList();
            Value[] row = new Value[columnCount];
            for (int j = 0; select.groupIndex != null && j < select.groupIndex.length; j++) {
                row[select.groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if (select.groupByExpression != null && select.groupByExpression[j]) {
                    continue;
                }
                Expression expr = select.expressions.get(j);
                row[j] = expr.getValue(session);
            }
            if (isHavingNullOrFalse(row, select.havingIndex)) {
                continue;
            }
            row = toResultRow(row, columnCount, select.resultColumnCount);
            result.addRow(row);
        }
        loopEnd = true;
    }

    static boolean isHavingNullOrFalse(Value[] row, int havingIndex) {
        if (havingIndex >= 0) {
            Value v = row[havingIndex];
            if (v == ValueNull.INSTANCE)
                return true;
            return !v.getBoolean();
        }
        return false;
    }

    // 不包含having和group by中加入的列
    static Value[] toResultRow(Value[] row, int columnCount, int resultColumnCount) {
        if (columnCount == resultColumnCount) {
            return row;
        }
        return Arrays.copyOf(row, resultColumnCount);
    }
}

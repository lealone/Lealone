/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import java.util.Arrays;
import java.util.HashMap;

import org.lealone.db.result.ResultTarget;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.expression.Expression;

// 只处理group by，且group by的字段没有索引
class QGroup extends QOperator {

    private final ValueHashMap<HashMap<Expression, Object>> groups;

    QGroup(Select select) {
        super(select);
        select.currentGroup = null;
        groups = ValueHashMap.newInstance();
    }

    public ValueHashMap<HashMap<Expression, Object>> getGroups() {
        return groups;
    }

    @Override
    public void run() {
        while (select.topTableFilter.next()) {
            boolean yield = yieldIfNeeded(++loopCount);
            if (conditionEvaluator.getBooleanValue()) {
                if (select.isForUpdate && !select.topTableFilter.lockRow())
                    return; // 锁记录失败
                rowCount++;
                Value key = getKey(select);
                select.currentGroup = getOrCreateGroup(groups, key);
                select.currentGroupRowId++;
                updateAggregate(select, columnCount);
                if (sampleSize > 0 && rowCount >= sampleSize) {
                    break;
                }
            }
            if (yield)
                return;
        }
        // 把分组后的记录放到result中
        addGroupRows(groups, select, columnCount, result);
        loopEnd = true;
    }

    static Value getKey(Select select) {
        // 避免在ExpressionColumn.getValue中取到旧值
        // 例如SELECT id/3 AS A, COUNT(*) FROM mytable GROUP BY A HAVING A>=0
        select.currentGroup = null;
        return ValueArray.get(getKeyValues(select));
    }

    // 分组key，包括一到多个字段
    static Value[] getKeyValues(Select select) {
        Value[] keyValues = new Value[select.groupIndex.length];
        for (int i = 0; i < select.groupIndex.length; i++) {
            int idx = select.groupIndex[i];
            Expression expr = select.expressions.get(idx);
            keyValues[i] = expr.getValue(select.getSession());
        }
        return keyValues;
    }

    static HashMap<Expression, Object> getOrCreateGroup(ValueHashMap<HashMap<Expression, Object>> groups, Value key) {
        HashMap<Expression, Object> values = groups.get(key);
        if (values == null) {
            values = new HashMap<>();
            groups.put(key, values);
        }
        return values;
    }

    static void addGroupRows(ValueHashMap<HashMap<Expression, Object>> groups, Select select, int columnCount,
            ResultTarget result) {
        for (Value v : groups.keys()) {
            ValueArray key = (ValueArray) v;
            select.currentGroup = groups.get(key);
            Value[] keyValues = key.getList();
            addGroupRow(select, keyValues, columnCount, result);
        }
    }

    static void addGroupRow(Select select, Value[] keyValues, int columnCount, ResultTarget result) {
        Value[] row = new Value[columnCount];
        for (int i = 0; select.groupIndex != null && i < select.groupIndex.length; i++) {
            row[select.groupIndex[i]] = keyValues[i];
        }
        for (int i = 0; i < columnCount; i++) {
            if (select.groupByExpression != null && select.groupByExpression[i]) {
                continue;
            }
            Expression expr = select.expressions.get(i);
            row[i] = expr.getValue(select.getSession());
        }
        if (isHavingNullOrFalse(row, select.havingIndex)) {
            return;
        }
        row = toResultRow(row, columnCount, select.resultColumnCount);
        result.addRow(row);
    }

    private static boolean isHavingNullOrFalse(Value[] row, int havingIndex) {
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

    static void updateAggregate(Select select, int columnCount) {
        for (int i = 0; i < columnCount; i++) {
            if (select.groupByExpression == null || !select.groupByExpression[i]) {
                Expression expr = select.expressions.get(i);
                expr.updateAggregate(select.getSession());
            }
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.sql.query;

import java.util.ArrayList;
import java.util.HashMap;

import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.sql.expression.Expression;

// 除了QuickAggregateQuery和GroupSortedQuery外，其他场景的聚合函数、group by、having都在这里处理
// groupIndex和groupByExpression为null的时候，表示没有group by
class QGroup extends QOperator {

    ValueHashMap<HashMap<Expression, Object>> groups;
    ValueArray defaultGroup;

    QGroup(Select select) {
        super(select);
    }

    @Override
    void start() {
        super.start();
        groups = ValueHashMap.newInstance();
        select.currentGroup = null;
        defaultGroup = ValueArray.get(new Value[0]);
    }

    @Override
    void run() {
        while (select.topTableFilter.next()) {
            boolean yieldIfNeeded = select.setCurrentRowNumber(rowNumber + 1);
            if (select.condition == null || select.condition.getBooleanValue(session)) {
                Value key;
                rowNumber++;
                if (select.groupIndex == null) {
                    key = defaultGroup;
                } else {
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
                    key = ValueArray.get(keyValues);
                }
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
                if (async && yieldIfNeeded)
                    return;
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        if (select.groupIndex == null && groups.size() == 0) {
            groups.put(defaultGroup, new HashMap<Expression, Object>());
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
            if (select.isHavingNullOrFalse(row)) {
                continue;
            }
            row = select.keepOnlyDistinct(row, columnCount);
            result.addRow(row);
        }
        loopEnd = true;
    }
}

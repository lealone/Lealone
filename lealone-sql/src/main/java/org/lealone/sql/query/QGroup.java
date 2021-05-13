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
        this.select.currentGroup = null;
        defaultGroup = ValueArray.get(new Value[0]);
    }

    @Override
    void run() {
        while (this.select.topTableFilter.next()) {
            boolean yieldIfNeeded = this.select.setCurrentRowNumber(rowNumber + 1);
            if (this.select.condition == null || this.select.condition.getBooleanValue(session)) {
                Value key;
                rowNumber++;
                if (this.select.groupIndex == null) {
                    key = defaultGroup;
                } else {
                    // 避免在ExpressionColumn.getValue中取到旧值
                    // 例如SELECT id/3 AS A, COUNT(*) FROM mytable GROUP BY A HAVING A>=0
                    this.select.currentGroup = null;
                    Value[] keyValues = new Value[this.select.groupIndex.length];
                    // update group
                    for (int i = 0; i < this.select.groupIndex.length; i++) {
                        int idx = this.select.groupIndex[i];
                        Expression expr = this.select.expressions.get(idx);
                        keyValues[i] = expr.getValue(session);
                    }
                    key = ValueArray.get(keyValues);
                }
                HashMap<Expression, Object> values = groups.get(key);
                if (values == null) {
                    values = new HashMap<Expression, Object>();
                    groups.put(key, values);
                }
                this.select.currentGroup = values;
                this.select.currentGroupRowId++;
                for (int i = 0; i < columnCount; i++) {
                    if (this.select.groupByExpression == null || !this.select.groupByExpression[i]) {
                        Expression expr = this.select.expressions.get(i);
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
        if (this.select.groupIndex == null && groups.size() == 0) {
            groups.put(defaultGroup, new HashMap<Expression, Object>());
        }
        ArrayList<Value> keys = groups.keys();
        for (Value v : keys) {
            ValueArray key = (ValueArray) v;
            this.select.currentGroup = groups.get(key);
            Value[] keyValues = key.getList();
            Value[] row = new Value[columnCount];
            for (int j = 0; this.select.groupIndex != null && j < this.select.groupIndex.length; j++) {
                row[this.select.groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if (this.select.groupByExpression != null && this.select.groupByExpression[j]) {
                    continue;
                }
                Expression expr = this.select.expressions.get(j);
                row[j] = expr.getValue(session);
            }
            if (this.select.isHavingNullOrFalse(row)) {
                continue;
            }
            row = this.select.keepOnlyDistinct(row, columnCount);
            result.addRow(row);
        }
        loopEnd = true;
    }
}

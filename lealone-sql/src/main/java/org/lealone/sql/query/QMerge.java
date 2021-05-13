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

import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.sql.expression.Calculator;
import org.lealone.sql.expression.Expression;

class QMerge extends QOperator {

    QMerge(Select select) {
        super(select);
    }

    public Result queryGroupMerge() {
        int columnCount = this.select.expressions.size();
        LocalResult result = new LocalResult(session, this.select.expressionArray, columnCount);
        ValueHashMap<HashMap<Expression, Object>> groups = ValueHashMap.newInstance();
        int rowNumber = 0;
        this.select.setCurrentRowNumber(0);
        ValueArray defaultGroup = ValueArray.get(new Value[0]);
        this.select.topTableFilter.reset();
        int sampleSize = this.select.getSampleSizeValue(session);
        while (this.select.topTableFilter.next()) {
            this.select.setCurrentRowNumber(rowNumber + 1);
            Value key;
            rowNumber++;
            if (this.select.groupIndex == null) {
                key = defaultGroup;
            } else {
                Value[] keyValues = new Value[this.select.groupIndex.length];
                // update group
                for (int i = 0; i < this.select.groupIndex.length; i++) {
                    int idx = this.select.groupIndex[i];
                    keyValues[i] = this.select.topTableFilter.getValue(idx);
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
                    expr.mergeAggregate(session, this.select.topTableFilter.getValue(i));
                }
            }
            if (sampleSize > 0 && rowNumber >= sampleSize) {
                break;
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
                row[j] = expr.getMergedValue(session);
            }
            result.addRow(row);
        }

        return result;
    }

    public Result calculate(Result result, Select select) {
        int size = this.select.expressions.size();
        if (this.select.havingIndex >= 0)
            size--;
        if (size == select.expressions.size())
            return result;

        int columnCount = this.select.visibleColumnCount;
        LocalResult lr = new LocalResult(session, this.select.expressionArray, columnCount);

        Calculator calculator;
        int index = 0;
        while (result.next()) {
            calculator = new Calculator(result.currentRow());
            for (int i = 0; i < columnCount; i++) {
                Expression expr = this.select.expressions.get(i);
                index = calculator.getIndex();
                expr.calculate(calculator);
                if (calculator.getIndex() == index) {
                    calculator.addResultValue(calculator.getValue(index));
                    calculator.addIndex();
                }
            }

            lr.addRow(calculator.getResult().toArray(new Value[0]));
        }

        return lr;
    }
}

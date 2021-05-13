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
        int columnCount = select.expressions.size();
        LocalResult result = new LocalResult(session, select.expressionArray, columnCount);
        ValueHashMap<HashMap<Expression, Object>> groups = ValueHashMap.newInstance();
        int rowNumber = 0;
        select.setCurrentRowNumber(0);
        ValueArray defaultGroup = ValueArray.get(new Value[0]);
        select.topTableFilter.reset();
        int sampleSize = select.getSampleSizeValue(session);
        while (select.topTableFilter.next()) {
            select.setCurrentRowNumber(rowNumber + 1);
            Value key;
            rowNumber++;
            if (select.groupIndex == null) {
                key = defaultGroup;
            } else {
                Value[] keyValues = new Value[select.groupIndex.length];
                // update group
                for (int i = 0; i < select.groupIndex.length; i++) {
                    int idx = select.groupIndex[i];
                    keyValues[i] = select.topTableFilter.getValue(idx);
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
                    expr.mergeAggregate(session, select.topTableFilter.getValue(i));
                }
            }
            if (sampleSize > 0 && rowNumber >= sampleSize) {
                break;
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
                row[j] = expr.getMergedValue(session);
            }
            result.addRow(row);
        }

        return result;
    }

    public Result calculate(Result result, Select newSelect) {
        int size = select.expressions.size();
        if (select.havingIndex >= 0)
            size--;
        if (size == newSelect.expressions.size())
            return result;

        int columnCount = select.visibleColumnCount;
        LocalResult lr = new LocalResult(session, select.expressionArray, columnCount);

        Calculator calculator;
        int index = 0;
        while (result.next()) {
            calculator = new Calculator(result.currentRow());
            for (int i = 0; i < columnCount; i++) {
                Expression expr = select.expressions.get(i);
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

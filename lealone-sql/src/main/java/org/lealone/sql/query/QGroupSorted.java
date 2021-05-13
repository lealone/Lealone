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

import java.util.Arrays;
import java.util.HashMap;

import org.lealone.db.result.ResultTarget;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;

class QGroupSorted extends QOperator {

    Value[] previousKeyValues;

    QGroupSorted(Select select) {
        super(select);
    }

    @Override
    void start() {
        super.start();
        select.currentGroup = null;
    }

    @Override
    void run() {
        while (select.topTableFilter.next()) {
            boolean yieldIfNeeded = select.setCurrentRowNumber(rowNumber + 1);
            if (select.condition == null || select.condition.getBooleanValue(session)) {
                rowNumber++;
                Value[] keyValues = new Value[select.groupIndex.length];
                // update group
                for (int i = 0; i < select.groupIndex.length; i++) {
                    int idx = select.groupIndex[i];
                    Expression expr = select.expressions.get(idx);
                    keyValues[i] = expr.getValue(session);
                }

                if (previousKeyValues == null) {
                    previousKeyValues = keyValues;
                    select.currentGroup = new HashMap<>();
                } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    addGroupSortedRow(previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    select.currentGroup = new HashMap<>();
                }
                select.currentGroupRowId++;

                for (int i = 0; i < columnCount; i++) {
                    if (!select.groupByExpression[i]) {
                        Expression expr = select.expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
                if (async && yieldIfNeeded)
                    return;
            }
        }
        if (previousKeyValues != null) {
            addGroupSortedRow(previousKeyValues, columnCount, result);
        }
        loopEnd = true;
    }

    private void addGroupSortedRow(Value[] keyValues, int columnCount, ResultTarget result) {
        Value[] row = new Value[columnCount];
        for (int j = 0; j < select.groupIndex.length; j++) {
            row[select.groupIndex[j]] = keyValues[j];
        }
        for (int j = 0; j < columnCount; j++) {
            if (select.groupByExpression[j]) {
                continue;
            }
            Expression expr = select.expressions.get(j);
            row[j] = expr.getValue(session);
        }
        if (select.isHavingNullOrFalse(row)) {
            return;
        }
        row = select.keepOnlyDistinct(row, columnCount);
        result.addRow(row);
    }
}

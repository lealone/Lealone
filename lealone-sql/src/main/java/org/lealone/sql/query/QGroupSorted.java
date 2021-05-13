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
        this.select.currentGroup = null;
    }

    @Override
    void run() {
        while (this.select.topTableFilter.next()) {
            boolean yieldIfNeeded = this.select.setCurrentRowNumber(rowNumber + 1);
            if (this.select.condition == null || this.select.condition.getBooleanValue(session)) {
                rowNumber++;
                Value[] keyValues = new Value[this.select.groupIndex.length];
                // update group
                for (int i = 0; i < this.select.groupIndex.length; i++) {
                    int idx = this.select.groupIndex[i];
                    Expression expr = this.select.expressions.get(idx);
                    keyValues[i] = expr.getValue(session);
                }

                if (previousKeyValues == null) {
                    previousKeyValues = keyValues;
                    this.select.currentGroup = new HashMap<>();
                } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    addGroupSortedRow(previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    this.select.currentGroup = new HashMap<>();
                }
                this.select.currentGroupRowId++;

                for (int i = 0; i < columnCount; i++) {
                    if (!this.select.groupByExpression[i]) {
                        Expression expr = this.select.expressions.get(i);
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
        for (int j = 0; j < this.select.groupIndex.length; j++) {
            row[this.select.groupIndex[j]] = keyValues[j];
        }
        for (int j = 0; j < columnCount; j++) {
            if (this.select.groupByExpression[j]) {
                continue;
            }
            Expression expr = this.select.expressions.get(j);
            row[j] = expr.getValue(session);
        }
        if (this.select.isHavingNullOrFalse(row)) {
            return;
        }
        row = this.select.keepOnlyDistinct(row, columnCount);
        result.addRow(row);
    }
}

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

import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;

//最普通的查询
class QFlat extends QOperator {

    QFlat(Select select) {
        super(select);
    }

    @Override
    void run() {
        while (select.topTableFilter.next()) {
            ++loopCount;
            if (select.condition == null || select.condition.getBooleanValue(session)) {
                if (select.isForUpdate && !select.topTableFilter.lockRow()) {
                    // 锁记录失败
                    return;
                }
                Value[] row = new Value[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    Expression expr = select.expressions.get(i);
                    row[i] = expr.getValue(session);
                }
                result.addRow(row);
                rowCount++;
                if (canBreakLoop()) {
                    break;
                }
            }
            if (yieldIfNeeded(loopCount))
                return;
        }
        loopEnd = true;
    }
}

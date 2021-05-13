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

// 对min、max、count三个聚合函数的特殊优化
class QQuick extends QOperator {

    QQuick(Select select) {
        super(select);
    }

    @Override
    void start() {
        // 什么都不需要做
    }

    @Override
    void run() {
        Value[] row = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Expression expr = select.expressions.get(i);
            row[i] = expr.getValue(session);
        }
        result.addRow(row);
        rowCount = 1;
        loopEnd = true;
    }
}

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

import org.lealone.db.index.Cursor;
import org.lealone.db.index.Index;
import org.lealone.db.result.SearchRow;
import org.lealone.db.value.Value;

// 单字段distinct
class QDistinct extends QOperator {

    private Index index;
    private int columnIndex;
    private Cursor cursor;

    QDistinct(Select select) {
        super(select);
    }

    @Override
    void start() {
        super.start();
        index = select.topTableFilter.getIndex();
        columnIndex = index.getColumns()[0].getColumnId();
        cursor = index.findDistinct(session, null, null);
    }

    @Override
    void run() {
        while (cursor.next()) {
            ++loopCount;
            SearchRow found = cursor.getSearchRow();
            Value value = found.getValue(columnIndex);
            Value[] row = { value };
            result.addRow(row);
            rowCount++;
            if (canBreakLoop()) {
                break;
            }
            if (yieldIfNeeded(loopCount))
                return;
        }
        loopEnd = true;
    }
}

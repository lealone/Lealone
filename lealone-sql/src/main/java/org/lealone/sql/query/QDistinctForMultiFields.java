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

class QDistinctForMultiFields extends QOperator {

    Index index;
    int[] columnIds;
    int size;
    Cursor cursor;

    QDistinctForMultiFields(Select select) {
        super(select);
    }

    @Override
    void start() {
        super.start();
        index = select.topTableFilter.getIndex();
        columnIds = index.getColumnIds();
        size = columnIds.length;
        cursor = index.findDistinct(session, null, null);
    }

    @Override
    void run() {
        while (cursor.next()) {
            boolean yieldIfNeeded = select.setCurrentRowNumber(rowNumber + 1);
            SearchRow found = cursor.getSearchRow();
            Value[] row = new Value[size];
            for (int i = 0; i < size; i++) {
                row[i] = found.getValue(columnIds[i]);
            }
            result.addRow(row);
            rowNumber++;
            if (async && yieldIfNeeded)
                return;
            if ((select.sort == null || select.sortUsingIndex) && limitRows > 0 && rowNumber >= limitRows) {
                break;
            }
            if (sampleSize > 0 && rowNumber >= sampleSize) {
                break;
            }
        }
        loopEnd = true;
    }
}

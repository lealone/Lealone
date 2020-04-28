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
package org.lealone.db.table;

import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.sql.SQLStatement;

public class TableAlterHistoryRecord {

    // private final int id;
    // private final int version;
    private final int alterType;
    private final String columns;

    public TableAlterHistoryRecord(int id, int version, int alterType, String columns) {
        // this.id = id;
        // this.version = version;
        this.alterType = alterType;
        this.columns = columns;
    }

    public Value[] redo(ServerSession session, Value[] values) {
        if (alterType == SQLStatement.ALTER_TABLE_DROP_COLUMN) {
            int position = Integer.parseInt(columns);
            int len = values.length;
            if (len == 1)
                return new Value[0];
            Value[] newValues = new Value[len - 1];
            System.arraycopy(values, 0, newValues, 0, position);
            System.arraycopy(values, position + 1, newValues, position, len - position - 1);
            return newValues;
        } else if (alterType == SQLStatement.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE) {
            int index = columns.indexOf(',');
            int position = Integer.parseInt(columns.substring(0, index));
            Column column = (Column) session.getParser().parseColumnForTable(columns.substring(index + 1));
            values[position] = column.convert(values[position]);
            return values;
        } else if (alterType == SQLStatement.ALTER_TABLE_ADD_COLUMN) {
            String[] a = columns.split(",");
            int position = Integer.parseInt(a[0]);
            int len = a.length - 1 + values.length;
            Value[] newValues = new Value[len];
            System.arraycopy(values, 0, newValues, 0, position);
            System.arraycopy(values, position, newValues, position + a.length - 1, values.length - position);
            Row row = new Row(newValues, 0);
            for (int i = 1; i < a.length; i++) {
                Column column = (Column) session.getParser().parseColumnForTable(a[i]);
                Value value = null;
                Value v2;
                if (column.getComputed()) {
                    // force updating the value
                    value = null;
                    v2 = column.computeValue(session, row);
                }
                v2 = column.validateConvertUpdateSequence(session, value);
                if (v2 != value) {
                    value = v2;
                }
                newValues[position++] = value;
            }
            return newValues;
        }

        return values;
    }
}

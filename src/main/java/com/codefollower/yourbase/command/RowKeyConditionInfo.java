/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.codefollower.yourbase.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.h2.command.dml.Select;
import com.codefollower.h2.expression.Comparison;
import com.codefollower.h2.table.Column;
import com.codefollower.h2.table.TableFilter;
import com.codefollower.h2.util.StatementBuilder;
import com.codefollower.h2.value.Value;
import com.codefollower.yourbase.table.HBaseTable;
import com.codefollower.yourbase.util.HBaseUtils;

public class RowKeyConditionInfo {
    private final HBaseTable table;

    private final Select select;
    private final String rowKeyName;
    private final byte[] tableName;

    private int compareTypeStart = -1;
    private Value compareValueStart;
    private int compareTypeStop = -1;
    private Value compareValueStop;

    private List<byte[]> startKeys;
    private byte[] startKey;
    private byte[] endKey;

    public RowKeyConditionInfo(HBaseTable table, Select select) {
        this.table = table;
        this.select = select;
        this.rowKeyName = table.getRowKeyName();
        this.tableName = Bytes.toBytes(table.getName());
    }

    public HBaseTable getTable() {
        return table;
    }

    public String getRowKeyName() {
        return rowKeyName;
    }

    public int getCompareTypeStart() {
        return compareTypeStart;
    }

    public void setCompareTypeStart(int compareTypeStart) {
        this.compareTypeStart = compareTypeStart;
    }

    public Value getCompareValueStart() {
        return compareValueStart;
    }

    public void setCompareValueStart(Value compareValueStart) {
        this.compareValueStart = compareValueStart;
    }

    public int getCompareTypeStop() {
        return compareTypeStop;
    }

    public void setCompareTypeStop(int compareTypeStop) {
        this.compareTypeStop = compareTypeStop;
    }

    public Value getCompareValueStop() {
        return compareValueStop;
    }

    public void setCompareValueStop(Value compareValueStop) {
        this.compareValueStop = compareValueStop;
    }

    public String[] getRowKeys() {
        String[] rowKeys = new String[2];
        if (getCompareValueStart() != null)
            rowKeys[0] = getCompareValueStart().getString();
        if (getCompareValueStop() != null)
            rowKeys[1] = getCompareValueStop().getString();
        return rowKeys;
    }

    public List<byte[]> getStartKeys() {
        if (startKeys == null) {
            String[] rowKeys = getRowKeys();
            if (rowKeys != null) {
                if (rowKeys.length >= 1 && rowKeys[0] != null)
                    startKey = Bytes.toBytes(rowKeys[0]);

                if (rowKeys.length >= 2 && rowKeys[1] != null)
                    endKey = Bytes.toBytes(rowKeys[1]);
            }

            if (startKey == null)
                startKey = HConstants.EMPTY_START_ROW;
            if (endKey == null)
                endKey = HConstants.EMPTY_END_ROW;
            try {
                startKeys = HBaseUtils.getStartKeysInRange(tableName, startKey, endKey);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return startKeys;
    }

    public String getRowKey() {
        String rowKey = null;

        if (getCompareTypeStart() != Comparison.EQUAL)
            throw new RuntimeException("rowKey compare type is not '='");
        if (getCompareValueStart() != null)
            rowKey = getCompareValueStart().getString();

        return rowKey;
    }

    public String[] getPlanSQLs() {
        getStartKeys();

        String[] sqls = null;
        if (startKeys != null) {
            boolean isDistributed = true;
            if (startKeys.size() == 1) {
                isDistributed = false;
            }

            int size = startKeys.size();
            sqls = new String[size];
            for (int i = 0; i < size; i++) {
                sqls[i] = select.getPlanSQL(startKeys.get(i), endKey, isDistributed);
            }
        }

        return sqls;
    }

    public String[] getPlanSQLs(TableFilter filter) {
        getStartKeys();

        String[] sqls = null;
        if (startKeys != null) {
            ArrayList<Column> columns = filter.getSelect().getColumns(filter);
            StatementBuilder buff = new StatementBuilder("SELECT " + rowKeyName);
            for (Column c : columns) {
                //buff.appendExceptFirst(", ");
                buff.append(", ");
                buff.append(c.getSQL());
            }
            buff.append(" FROM ");
            buff.append(filter.getPlanSQL(false));
            buff.append(" WHERE 1=1");
            int size = startKeys.size();
            sqls = new String[size];
            StatementBuilder buff2 = new StatementBuilder();
            for (int i = 0; i < size; i++) {
                byte[] startKey = startKeys.get(i);
                if ((startKey != null && startKey.length > 0) || (endKey != null && endKey.length > 0)) {
                    if (startKey != null && startKey.length > 0) {
                        buff2.append(" AND ");
                        if (getCompareTypeStart() == Comparison.EQUAL)
                            buff2.append(rowKeyName).append(" = '").append(Bytes.toString(startKey)).append("'");
                        else
                            buff2.append(rowKeyName).append(" >= '").append(Bytes.toString(startKey)).append("'");
                    }
                    if (endKey != null && endKey.length > 0) {
                        buff2.append(" AND ");
                        buff2.append(rowKeyName).append(" < '").append(Bytes.toString(endKey)).append("'");
                    }
                }
                sqls[i] = buff + buff2.toString();
            }
        }

        return sqls;
    }

    public int getStartKeysSize() {
        getStartKeys();
        if (startKeys == null)
            return 0;
        else
            return startKeys.size();
    }
}

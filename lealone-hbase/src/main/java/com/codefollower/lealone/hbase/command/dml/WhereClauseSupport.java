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
package com.codefollower.lealone.hbase.command.dml;

import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.dbobject.table.TableFilter;
import com.codefollower.lealone.dbobject.table.TableView;
import com.codefollower.lealone.hbase.dbobject.index.HBaseSecondaryIndex;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueBytes;

public class WhereClauseSupport {
    private TableFilter tableFilter;
    private byte[] tableNameAsBytes;
    private String regionName;

    public WhereClauseSupport() {
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
        if (tableFilter.getTable() instanceof TableView)
            tableNameAsBytes = Bytes.toBytes(((TableView) tableFilter.getTable()).getTableName());
        else
            tableNameAsBytes = ((HBaseTable) tableFilter.getTable()).getTableNameAsBytes();
    }

    public TableFilter getTableFilter() {
        return tableFilter;
    }

    public byte[] getTableNameAsBytes() {
        if (tableFilter.getIndex() instanceof HBaseSecondaryIndex) {
            return ((HBaseSecondaryIndex) tableFilter.getIndex()).getTableNameAsBytes();
        }
        return tableNameAsBytes;
    }

    public Value getStartRowKeyValue() {
        SearchRow start = tableFilter.getStartSearchRow();

        if (start != null) {
            if (tableFilter.getIndex() instanceof HBaseSecondaryIndex) {
                return ValueBytes.getNoCopy(((HBaseSecondaryIndex) tableFilter.getIndex()).getKey(start));
            }
            return start.getRowKey();
        }
        return null;
    }

    public Value getEndRowKeyValue() {
        SearchRow end = tableFilter.getEndSearchRow();
        if (end != null) {
            if (tableFilter.getIndex() instanceof HBaseSecondaryIndex) {
                return ValueBytes.getNoCopy(((HBaseSecondaryIndex) tableFilter.getIndex()).getKey(end));
            }
            return end.getRowKey();
        }
        return null;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }
}

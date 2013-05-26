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
package com.codefollower.lealone.hbase.dbobject.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import com.codefollower.lealone.constant.Constants;
import com.codefollower.lealone.dbobject.index.BaseIndex;
import com.codefollower.lealone.dbobject.index.Cursor;
import com.codefollower.lealone.dbobject.index.IndexType;
import com.codefollower.lealone.dbobject.table.IndexColumn;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.dbobject.table.TableFilter;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.result.SortOrder;

public class HBasePrimaryIndex extends BaseIndex {
    public HBasePrimaryIndex(Table table, int id, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
    }

    @Override
    public void close(Session session) {
    }

    @Override
    public void add(Session session, Row row) {
        try {
            ((HBaseSession) session).getRegionServer().put(((HBaseRow) row).getRegionName(), ((HBaseRow) row).getPut());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove(Session session, Row row) {
        if (((HBaseRow) row).isForUpdate()) //Update这种类型的SQL不需要先删除再insert，只需直接insert即可
            return;
        try {
            Delete delete;
            Result result = ((HBaseRow) row).getResult();
            if (result != null) { //delete from语句需要先把要删除的记录get或scan出来，此时用原始的Result的时间戳
                delete = new Delete(result.getRow());
                for (KeyValue kv : result.list()) {
                    delete.deleteColumn(kv.getFamily(), kv.getQualifier(), kv.getTimestamp());
                }
            } else { //rollback的场景
                Put put = ((HBaseRow) row).getPut();
                delete = new Delete(put.getRow());
                long timestamp = row.getTransactionId();
                for (Map.Entry<byte[], List<KeyValue>> e : put.getFamilyMap().entrySet()) {
                    for (KeyValue kv : e.getValue()) {
                        delete.deleteColumn(e.getKey(), kv.getQualifier(), timestamp);
                    }
                }
            }

            ((HBaseSession) session).getRegionServer().delete(((HBaseRow) row).getRegionName(), delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return new HBasePrimaryIndexCursor(filter, first, last);
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        throw DbException.getUnsupportedException("find(Session, SearchRow, SearchRow)");
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        return 10 * Constants.COST_ROW_OFFSET + 100;
    }

    @Override
    public void remove(Session session) {
    }

    @Override
    public void truncate(Session session) {
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("findFirstOrLast");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        return 0;
    }

    @Override
    public void checkRename() {
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }
}

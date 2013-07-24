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
    public void add(Session session, Row row) {
        try {
            //分两种场景:
            //1. 以insert into这类SQL语句插入记录
            //   这种场景用正常的Put操作
            //
            //2. 以delete from这类SQL语句删除记录时出错了
            //   这种方式实际上并不真的删除原有记录，而是插入一条值为null的新版本记录，出错时撤消，
            //   所以要用Delete
            Result result = ((HBaseRow) row).getResult();
            if (result == null) {
                ((HBaseSession) session).getRegionServer().put(((HBaseRow) row).getRegionName(), ((HBaseRow) row).getPut());
            } else {
                Delete delete = new Delete(result.getRow());
                long timestamp = row.getTransactionId();
                for (KeyValue kv : result.list()) {
                    delete.deleteColumn(kv.getFamily(), kv.getQualifier(), timestamp);
                }
                ((HBaseSession) session).getRegionServer().delete(((HBaseRow) row).getRegionName(), delete);
            }
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void remove(Session session, Row row) {
        if (((HBaseRow) row).isForUpdate()) //Update这种类型的SQL不需要先删除再insert，只需直接insert即可
            return;
        try {
            //分两种场景:
            //1. 以delete from这类SQL语句删除记录
            //   这种场景会把要删除的记录找出来，此时用put的方式，不能直接用Delete，因为用Delete后如果当前事务未提交
            //   那么其它并发事务就找不到之前的记录版本
            //
            //2. 在进行insert时出错了
            //   比如此索引后面有一个唯一索引，往唯一索引insert了重复值，那么就出错，此时立即撤消，
            //   因为是新记录，所以要用Delete
            Result result = ((HBaseRow) row).getResult();
            if (result != null) {
                Put put = new Put(result.getRow());
                long timestamp = row.getTransactionId();
                for (KeyValue kv : result.list()) {
                    put.add(kv.getFamily(), kv.getQualifier(), timestamp, null);
                }
                ((HBaseSession) session).getRegionServer().put(((HBaseRow) row).getRegionName(), put);
            } else {
                Put oldPput = ((HBaseRow) row).getPut();
                Delete delete = new Delete(oldPput.getRow());
                long timestamp = row.getTransactionId();
                for (Map.Entry<byte[], List<KeyValue>> e : oldPput.getFamilyMap().entrySet()) {
                    for (KeyValue kv : e.getValue()) {
                        delete.deleteColumn(e.getKey(), kv.getQualifier(), timestamp);
                    }
                }
                ((HBaseSession) session).getRegionServer().delete(((HBaseRow) row).getRegionName(), delete);
            }
        } catch (IOException e) {
            throw DbException.convert(e);
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
    public void close(Session session) {
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

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
package org.lealone.hbase.dbobject.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.lealone.constant.Constants;
import org.lealone.dbobject.index.BaseIndex;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.Table;
import org.lealone.dbobject.table.TableFilter;
import org.lealone.engine.Session;
import org.lealone.hbase.dbobject.table.HBaseTable;
import org.lealone.hbase.engine.HBaseConstants;
import org.lealone.hbase.engine.HBaseSession;
import org.lealone.hbase.result.HBaseRow;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.result.SortOrder;

public class HBasePrimaryIndex extends BaseIndex {
    private final byte[] defaultColumnFamilyName;

    public HBasePrimaryIndex(Table table, int id, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
        defaultColumnFamilyName = ((HBaseTable) table).getDefaultColumnFamilyNameAsBytes();
    }

    @Override
    public void add(Session session, Row row) {
        try {
            ((HBaseSession) session).getRegionServer().put(((HBaseRow) row).getRegionName(), ((HBaseRow) row).getPut());
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void remove(Session session, Row row) {
        remove(session, row, false);
    }

    public void remove(Session session, Row row, boolean isUndo) {
        HBaseRow hr = (HBaseRow) row;
        if (hr.isForUpdate()) //Update这种类型的SQL不需要先删除再insert，只需直接insert即可
            return;
        try {
            HBaseSession hs = (HBaseSession) session;
            if (isUndo) {
                Put oldPut = hr.getPut();
                //撤消前面的add或remove操作
                if (oldPut != null) {
                    Delete delete = new Delete(oldPut.getRow());
                    for (Map.Entry<byte[], List<KeyValue>> e : oldPut.getFamilyMap().entrySet()) {
                        for (KeyValue kv : e.getValue()) {
                            delete.deleteColumn(e.getKey(), kv.getQualifier(), kv.getTimestamp());
                        }
                    }
                    hs.getRegionServer().delete(hr.getRegionName(), delete);
                } else
                    throw DbException.throwInternalError("oldPut is null???");
            } else {
                //正常的delete语句，
                //这种场景会把要删除的记录找出来，此时用Put的方式，不能直接用Delete，因为用Delete后如果当前事务未提交
                //那么其它并发事务就找不到之前的记录版本
                Result result = hr.getResult();
                if (result != null) {
                    Put put = hs.getTransaction().createHBasePutWithDeleteTag(defaultColumnFamilyName, result.getRow());

                    for (KeyValue kv : result.list()) {
                        if (Bytes.equals(kv.getQualifier(), HBaseConstants.TRANSACTION_META) //
                                && Bytes.equals(kv.getFamily(), defaultColumnFamilyName))
                            continue;
                        put.add(kv.getFamily(), kv.getQualifier(), null);
                    }
                    hs.getRegionServer().put(hr.getRegionName(), put);
                    hr.setPut(put);
                } else
                    throw DbException.throwInternalError("result is null???");
            }
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        //是一个join子查询
        if ((filter.getSelect() != null && filter.getSelect().getTopTableFilter() != filter))
            return new SubqueryCursor(filter, first, last);
        else
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

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
package com.codefollower.yourbase.hbase.command.dml;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.yourbase.api.Trigger;
import com.codefollower.yourbase.command.dml.Update;
import com.codefollower.yourbase.dbobject.Right;
import com.codefollower.yourbase.dbobject.table.Column;
import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.expression.Expression;
import com.codefollower.yourbase.expression.ValueExpression;
import com.codefollower.yourbase.hbase.command.HBasePrepared;
import com.codefollower.yourbase.hbase.engine.HBaseSession;
import com.codefollower.yourbase.hbase.util.HBaseUtils;
import com.codefollower.yourbase.result.SearchRow;
import com.codefollower.yourbase.value.Value;

//TODO
//可Update多条记录，但是因为不支持事务，所以有可能出现部分Update成功、部分Update失败。
public class HBaseUpdate extends Update implements HBasePrepared {
    //private final HBaseSession session;
    private String regionName;

    public HBaseUpdate(Session session) {
        super(session);
        //this.session = (HBaseSession) session;
    }

//    @Override
//    public int update() {
//        Table table = tableFilter.getTable();
//        session.getUser().checkRight(table, Right.UPDATE);
//        table.fire(session, Trigger.UPDATE, true);
//        table.lock(session, true, false);
//
//        setCurrentRowNumber(0);
//        if (getRowKey() == null)
//            return 0;
//
//        byte[] rowKey = Bytes.toBytes(getRowKey());
//        try {
//            setCurrentRowNumber(1);
//            Result result = session.getRegionServer().get(session.getRegionName(), new Get(rowKey));
//            Put put = new Put(rowKey);
//            Column c = null;
//            int columnCount = columns.size();
//            Value v = null;
//            for (KeyValue kv : result.raw()) {
//                for (int i = 0; i < columnCount; i++) {
//                    c = columns.get(i);
//                    if (Bytes.equals(c.getColumnFamilyNameAsBytes(), kv.getFamily()) //
//                            && Bytes.equals(c.getNameAsBytes(), kv.getQualifier())) {
//
//                        Expression newExpr = expressionMap.get(c);
//                        if (newExpr == null || newExpr == ValueExpression.getDefault()) {
//                            put.add(kv.getFamily(), kv.getQualifier(), kv.getValue());
//                        } else {
//                            v = newExpr.getValue(session);
//                            v = c.convert(v);
//                            put.add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp() + 1, HBaseUtils.toBytes(v));
//                        }
//
//                        break;
//                    }
//                }
//            }
//
//            RowMutations rm = new RowMutations(rowKey);
//            //rm.add(new org.apache.hadoop.hbase.client.Delete(rowKey));
//            rm.add(put);
//
//            session.getRegionServer().mutateRow(session.getRegionName(), rm);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        table.fire(session, Trigger.UPDATE, false);
//        return 1;
//
//    }

    @Override
    public void prepare() {
        super.prepare();
        tableFilter.setPrepared(this);
    }

    @Override
    public String getTableName() {
        return tableFilter.getTable().getName();
    }

    @Override
    public boolean isDistributedSQL() {
        return true;
    }

    @Override
    public Value getStartRowKeyValue() {
        SearchRow start = tableFilter.getStartSearchRow();
        if (start != null)
            return start.getRowKey();
        return null;
    }

    @Override
    public Value getEndRowKeyValue() {
        SearchRow end = tableFilter.getEndSearchRow();
        if (end != null)
            return end.getRowKey();
        return null;
    }

    @Override
    public String getRegionName() {
        return regionName;
    }

    @Override
    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }
}

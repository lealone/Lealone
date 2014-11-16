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
package org.lealone.cassandra.command;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.ResultSet.Metadata;
import org.apache.cassandra.db.marshal.AbstractType;
import org.lealone.result.ResultInterface;
import org.lealone.value.Value;
import org.lealone.value.ValueLong;
import org.lealone.value.ValueString;

public class CassandraResult implements ResultInterface {
    private ResultSet rs;
    private int size;
    private int index = 0;
    private Value[] currentRow;

    public CassandraResult(ResultSet rs) {
        this.rs = rs;
        size = rs.size();
    }

    @Override
    public void reset() {
        index = 0;
    }

    @Override
    public Value[] currentRow() {
        return currentRow;
    }

    @Override
    public boolean next() {
//        List<ByteBuffer> list = rs.rows.get(index);
//        currentRow = new Value[list.size()];
//        final Metadata metadata = rs.metadata;
//        ByteBuffer b;
//        for(int i= 0; i<list.size(); i++) {
//            b = list.get(i);
//            AbstractType<?> type = rs.metadata.names.get(i).type;
//            if(type.asCQL3Type() instanceof CQL3Type.Native) {
//               // CQL3Type.Native nType = (CQL3Type.Native)type.asCQL3Type();
//                switch ((CQL3Type.Native)type.asCQL3Type()) {
//                case ASCII:
//                    currentRow[i] = ValueString.get(type.getString(b));
//                    break;
//                case BIGINT:
//                    currentRow[i] = ValueLong.get(type).get(type.getString(b));
//                    break;
//                case BLOB:
//                    System.out.print(row.getLong(i));
//                    break;
//                default:
//                    System.out.print(row.getString(i));
//                    break;
//                }
//            }
//        }
        return index++ > size;
    }

    @Override
    public int getRowId() {
        return index;
    }

    @Override
    public int getVisibleColumnCount() {
        return getRowCount();
    }

    @Override
    public int getRowCount() {
        return size;
    }

    @Override
    public boolean needToClose() {
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public String getAlias(int i) {
        return null;
    }

    @Override
    public String getSchemaName(int i) {
        return null;
    }

    @Override
    public String getTableName(int i) {
        return null;
    }

    @Override
    public String getColumnName(int i) {
        return null;
    }

    @Override
    public int getColumnType(int i) {
        return 0;
    }

    @Override
    public long getColumnPrecision(int i) {
        return 0;
    }

    @Override
    public int getColumnScale(int i) {
        return 0;
    }

    @Override
    public int getDisplaySize(int i) {
        return 0;
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return false;
    }

    @Override
    public int getNullable(int i) {
        return 0;
    }

    @Override
    public void setFetchSize(int fetchSize) {
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

}

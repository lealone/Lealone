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
package com.codefollower.yourbase.hbase.command.ddl;

import com.codefollower.yourbase.command.ddl.AlterSequence;
import com.codefollower.yourbase.dbobject.Schema;
import com.codefollower.yourbase.dbobject.Sequence;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.hbase.dbobject.HBaseSequence;
import com.codefollower.yourbase.result.ResultInterface;
import com.codefollower.yourbase.value.Value;
import com.codefollower.yourbase.value.ValueLong;

public class AlterSequenceNextValueMargin extends AlterSequence {
    private HBaseSequence sequence;

    public AlterSequenceNextValueMargin(Session session, Schema schema, Sequence sequence) {
        super(session, schema);
        this.sequence = (HBaseSequence) sequence;
    }

    @Override
    public int update() {
        sequence.alterNextValueMargin(session);
        return 0;
    }

    public ResultInterface query(int maxrows) {
        return new ResultInterfaceImpl(sequence.alterNextValueMargin(session));
    }

    static class ResultInterfaceImpl implements ResultInterface {
        Value[] v = new Value[1];
        boolean next = true;

        ResultInterfaceImpl(long value) {
            v[0] = ValueLong.get(value);
        }

        @Override
        public void reset() {
        }

        @Override
        public Value[] currentRow() {
            return v;
        }

        @Override
        public boolean next() {
            boolean n = next;
            if (next) {
                next = false;
            }
            return n;
        }

        @Override
        public int getRowId() {
            return 0;
        }

        @Override
        public int getVisibleColumnCount() {
            return 1;
        }

        @Override
        public int getRowCount() {
            return 1;
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
}

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
package com.codefollower.yourbase.hbase.dbobject.index;

import com.codefollower.yourbase.dbobject.index.BaseIndex;
import com.codefollower.yourbase.dbobject.index.Cursor;
import com.codefollower.yourbase.dbobject.index.IndexType;
import com.codefollower.yourbase.dbobject.table.IndexColumn;
import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.dbobject.table.TableFilter;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.result.Row;
import com.codefollower.yourbase.result.SearchRow;

public class HBaseTableIndex extends BaseIndex {

    public HBaseTableIndex(Table table, int id, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
    }

    public HBaseTableIndex(Table table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, indexName, columns, indexType);
    }

    @Override
    public void close(Session session) {
    }

    @Override
    public void add(Session session, Row row) {
    }

    @Override
    public void remove(Session session, Row row) {
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return new HBaseTableCursor(filter, first, last);
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return new HBaseTableCursor(session, first, last);
    }

    @Override
    public double getCost(Session session, int[] masks) {
        return 0;
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
        return new HBaseTableCursor(session, null, null);
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

}

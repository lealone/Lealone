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
package org.lealone.cbase.dbobject.index;

import org.lealone.constant.ErrorCode;
import org.lealone.dbobject.index.BaseIndex;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.Table;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.result.SortOrder;
import org.lealone.value.Value;

public class CBaseSecondaryIndex extends BaseIndex {
    public CBaseSecondaryIndex(Table table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, indexName, columns, indexType);
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
    }

    private static void checkIndexColumnTypes(IndexColumn[] columns) {
        for (IndexColumn c : columns) {
            int type = c.column.getType();
            if (type == Value.CLOB || type == Value.BLOB) {
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1,
                        "Index on BLOB or CLOB column: " + c.column.getCreateSQL());
            }
        }
    }

    @Override
    public void close(Session session) {
        // TODO Auto-generated method stub

    }

    @Override
    public void add(Session session, Row row) {
        // TODO Auto-generated method stub

    }

    @Override
    public void remove(Session session, Row row) {
        // TODO Auto-generated method stub

    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void remove(Session session) {
        // TODO Auto-generated method stub

    }

    @Override
    public void truncate(Session session) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean canGetFirstOrLast() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean needRebuild() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void checkRename() {
        // TODO Auto-generated method stub

    }

}

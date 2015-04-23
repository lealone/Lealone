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
package org.lealone.wiredtiger.dbobject.index;

import org.lealone.dbobject.index.BaseIndex;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.TableFilter;
import org.lealone.engine.Constants;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.result.SortOrder;
import org.lealone.wiredtiger.dbobject.table.WiredTigerTable;

public class WiredTigerDelegateIndex extends BaseIndex {

    private final WiredTigerPrimaryIndex mainIndex;

    public WiredTigerDelegateIndex(WiredTigerTable table, int id, String name, IndexColumn[] cols,
            WiredTigerPrimaryIndex mainIndex, IndexType indexType) {
        if (id < 0)
            throw DbException.throwInternalError(name);
        this.initBaseIndex(table, id, name, cols, indexType);
        this.mainIndex = mainIndex;
    }

    @Override
    public void add(Session session, Row row) {
        // nothing to do
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return mainIndex.find(session, first, last);
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return mainIndex.find(filter, first, last);
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        return mainIndex.findFirstOrLast(session, first);
    }

    @Override
    public int getColumnIndex(Column col) {
        if (col.getColumnId() == columns[0].getColumnId()) {
            return 0;
        }
        return -1;
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        int mask = masks[columns[0].getColumnId()];
        if (mask != 0)
            return 3;
        else
            return 10 * Constants.COST_ROW_OFFSET;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void remove(Session session, Row row) {
        // nothing to do
    }

    @Override
    public void remove(Session session) {
        // nothing to do
    }

    @Override
    public void truncate(Session session) {
        // nothing to do
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public long getRowCount(Session session) {
        return mainIndex.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return mainIndex.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return mainIndex.getDiskSpaceUsed();
    }

}

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
package org.lealone.mysql.dbobject.index;

import org.lealone.dbobject.index.BaseIndex;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.engine.Session;
import org.lealone.mysql.dbobject.table.MySQLTable;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.result.SortOrder;

public class MySQLIndex extends BaseIndex {

    private final MySQLTable table;

    public MySQLIndex(MySQLTable table, int id, IndexColumn[] columns) {
        initBaseIndex(table, id, "MySQLIndex_INDEX", columns, IndexType.createNonUnique(true));
        this.table = table;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public void add(Session session, Row row) {
    }

    @Override
    public void remove(Session session, Row row) {
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return new MySQLCursor();
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        return 1;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public void remove(Session session) {
    }

    @Override
    public void truncate(Session session) {
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void checkRename() {
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        return new MySQLCursor();
    }

    @Override
    public long getRowCount(Session session) {
        return table.getRowCountApproximation();
    }

    @Override
    public long getRowCountApproximation() {
        return table.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    private static class MySQLCursor implements Cursor {

        @Override
        public Row get() {
            return null;
        }

        @Override
        public SearchRow getSearchRow() {
            return null;
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public boolean previous() {
            return false;
        }

    }
}

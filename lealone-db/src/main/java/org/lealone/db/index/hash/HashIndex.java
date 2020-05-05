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
package org.lealone.db.index.hash;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.IndexBase;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexConditionType;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;

public abstract class HashIndex extends IndexBase {

    /**
     * The index of the indexed column.
     */
    protected final int indexColumn;

    protected HashIndex(Table table, int id, String indexName, IndexType indexType, IndexColumn[] columns) {
        super(table, id, indexName, indexType, columns);
        this.indexColumn = columns[0].column.getColumnId();
        reset();
    }

    protected abstract void reset();

    @Override
    public void truncate(ServerSession session) {
        reset();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public void close(ServerSession session) {
        // nothing to do
    }

    @Override
    public void remove(ServerSession session) {
        // nothing to do
    }

    @Override
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexConditionType.EQUALITY) != IndexConditionType.EQUALITY) {
                return Long.MAX_VALUE;
            }
        }
        return 2;
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(ServerSession session, boolean first) {
        throw DbException.getUnsupportedException("HASH");
    }

    @Override
    public boolean canScan() {
        return false;
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    @Override
    public boolean isInMemory() {
        return true;
    }
}

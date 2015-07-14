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
package org.lealone.command.router;

import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.index.IndexBase;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.Table;
import org.lealone.dbobject.table.TableFilter;
import org.lealone.engine.Session;
import org.lealone.result.ResultInterface;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.result.SortOrder;

public class MergedIndex extends IndexBase {
    private final ResultInterface result;

    public MergedIndex(ResultInterface result, Table table, int id, IndexColumn[] columns, IndexType indexType) {
        super();
        this.result = result;
        initIndexBase(table, id, table.getName() + "_DATA", columns, indexType);
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return new MergedCursor(result);
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return new MergedCursor(result);
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter filter, SortOrder sortOrder) {
        return 0;
    }

    @Override
    public long getRowCount(Session session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        return 0;
    }

    private static class MergedCursor implements Cursor {
        private final ResultInterface result;

        MergedCursor(ResultInterface result) {
            this.result = result;
        }

        @Override
        public Row get() {
            return new Row(result.currentRow(), -1);
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            return result.next();
        }

        @Override
        public boolean previous() {
            return false;
        }
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query.sharding.result;

import org.lealone.db.index.Cursor;
import org.lealone.db.index.IndexBase;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.Result;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;

public class MergedIndex extends IndexBase {

    private final Result result;

    public MergedIndex(Result result, Table table, int id, IndexType indexType, IndexColumn[] columns) {
        super(table, id, table.getName() + "_DATA", indexType, columns);
        this.result = result;
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        return new MergedCursor(result);
    }

    @Override
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        return 0;
    }

    @Override
    public long getRowCount(ServerSession session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        return 0;
    }

    private static class MergedCursor implements Cursor {

        private final Result result;

        MergedCursor(Result result) {
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
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.optimizer;

import org.lealone.db.index.Cursor;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.storage.page.IPage;

public class TableIterator {

    private final ServerSession session;
    private final TableFilter tableFilter;
    private final Table table;
    private Row oldRow;
    private boolean hasNext;
    private Cursor cursor;

    public TableIterator(ServerSession session, TableFilter tableFilter) {
        this.session = session;
        this.tableFilter = tableFilter;
        this.table = tableFilter.getTable();
    }

    public void setCursor(Cursor cursor) {
        this.cursor = cursor;
    }

    public void start(long limitRows) {
        tableFilter.startQuery(session);
        tableFilter.reset();
        if (limitRows == 0)
            hasNext = false;
        else
            next(); // 提前next，当发生行锁时可以直接用tableFilter的当前值重试
    }

    public boolean hasNext() {
        return hasNext;
    }

    public boolean next() {
        if (cursor == null) {
            return hasNext = tableFilter.next();
        } else {
            return hasNext = cursor.next();
        }
    }

    public void rebuildSearchRowIfNeeded() {
        if (oldRow != null) {
            // 如果oldRow已经删除了那么移到下一行
            if (tableFilter.rebuildSearchRow(session, oldRow) == null)
                hasNext = tableFilter.next();
            oldRow = null;
        }
    }

    public Row getRow() {
        if (cursor == null) {
            return tableFilter.get();
        } else {
            SearchRow found = cursor.getSearchRow();
            return tableFilter.getTable().getRow(session, found.getKey());
        }
    }

    public int tryLockRow(int[] lockColumns) {
        Row oldRow = getRow();
        if (oldRow == null) { // 已经删除了
            next();
            return -1;
        }
        IPage page = oldRow.getPage();
        if (page != null) {
            if (!session.addLockedPage(page))
                return 0;
        }
        int ret = table.tryLockRow(session, oldRow, lockColumns);
        if (ret < 0) { // 已经删除了
            next();
            session.removeLockedPage(page);
            return -1;
        } else if (ret == 0) { // 被其他事务锁住了
            this.oldRow = oldRow;
            session.removeLockedPage(page);
            return 0;
        }
        if (table.isRowChanged(oldRow)) {
            tableFilter.rebuildSearchRow(session, oldRow);
            session.removeLockedPage(page);
            return -1;
        }
        return 1;
    }
}

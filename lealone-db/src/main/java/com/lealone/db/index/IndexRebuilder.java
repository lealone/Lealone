/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.trace.TraceModuleType;
import com.lealone.common.util.MathUtils;
import com.lealone.db.Database;
import com.lealone.db.api.DatabaseEventListener;
import com.lealone.db.async.AsyncPeriodicTask;
import com.lealone.db.row.Row;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;

public class IndexRebuilder implements Runnable {

    private final ServerSession session;
    private final Table table;
    private final Index index;

    private int rowCount;
    private Cursor cursor;
    private AsyncPeriodicTask task;

    public IndexRebuilder(ServerSession session, Table table, Index index) {
        this.session = session;
        this.table = table;
        this.index = index;
    }

    public void rebuild() {
        index.setBuilding(true);
        Index scan = table.getScanIndex(session);
        rowCount = MathUtils.convertLongToInt(table.getRowCount(session));
        cursor = scan.find(session, null, null);
        task = new AsyncPeriodicTask(0, 100, this);
        session.getScheduler().addPeriodicTask(task);
    }

    private void onComplete() {
        index.setBuilding(false);
        index.setLastIndexedRowKey(null);
        task.cancel();
        session.getScheduler().removePeriodicTask(task);
    }

    @Override
    public void run() {
        session.setUndoLogEnabled(false);
        try {
            long i = 0;
            String n = table.getName() + ":" + index.getName();
            Database database = table.getSchema().getDatabase();
            while (!index.isClosed() && cursor.next()) {
                Row row = cursor.get();
                index.add(session, row);
                index.setLastIndexedRowKey(row.getKey());
                if ((++i & 127) == 0) {
                    database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n,
                            MathUtils.convertLongToInt(i), rowCount);
                    if (session.getScheduler().yieldIfNeeded(null))
                        return;
                }
            }
            onComplete();
        } catch (DbException e) {
            onComplete();
            table.getSchema().freeUniqueName(index.getName());
            try {
                index.remove(session);
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means
                // there is something wrong with the database
                session.getTrace().setType(TraceModuleType.TABLE).error(e2, "could not remove index");
                throw e2;
            }
            throw e;
        } finally {
            session.setUndoLogEnabled(true);
        }
    }
}

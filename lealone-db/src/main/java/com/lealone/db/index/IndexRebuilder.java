/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.index;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.trace.TraceModuleType;
import com.lealone.common.util.MathUtils;
import com.lealone.db.Database;
import com.lealone.db.api.DatabaseEventListener;
import com.lealone.db.result.Row;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;

/**
 * @author H2 Group
 * @author zhh
 */
public class IndexRebuilder implements Runnable {

    private final ServerSession session;
    private final Table table;
    private final Index index;

    public IndexRebuilder(ServerSession session, Table table, Index index) {
        this.session = session;
        this.table = table;
        this.index = index;
    }

    @Override
    public void run() {
        rebuild();
    }

    public void rebuild() {
        session.setUndoLogEnabled(false);
        try {
            Index scan = table.getScanIndex(session);
            int rowCount = MathUtils.convertLongToInt(scan.getRowCount(session));
            long i = 0;
            String n = table.getName() + ":" + index.getName();
            Database database = table.getSchema().getDatabase();
            Cursor cursor = scan.find(session, null, null);
            while (cursor.next()) {
                Row row = cursor.get();
                index.add(session, row);
                if ((++i & 127) == 0) {
                    database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n,
                            MathUtils.convertLongToInt(i), rowCount);
                }
            }
        } catch (DbException e) {
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

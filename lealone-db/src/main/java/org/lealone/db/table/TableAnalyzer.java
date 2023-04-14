/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.table;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.lealone.common.util.StatementBuilder;
import org.lealone.db.CommandParameter;
import org.lealone.db.Database;
import org.lealone.db.auth.Right;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.PreparedSQLStatement;

public class TableAnalyzer {

    private final Table table;
    private final AtomicBoolean analyzing = new AtomicBoolean();
    private int nextAnalyze;
    private int changesSinceAnalyze;

    public TableAnalyzer(Table table, int nextAnalyze) {
        this.table = table;
        this.nextAnalyze = nextAnalyze;
    }

    public void reset() {
        changesSinceAnalyze = 0;
    }

    // 允许多线程运行，对changesSinceAnalyze计数虽然不是线程安全的，但不要求准确，所以不必用原子操作
    public void analyze(ServerSession session) {
        if (nextAnalyze > changesSinceAnalyze++) {
            return;
        }
        if (analyzing.compareAndSet(false, true)) {
            changesSinceAnalyze = 0;
            int n = 2 * nextAnalyze;
            if (n > 0) {
                nextAnalyze = n;
            }
            int rows = session.getDatabase().getSettings().analyzeSample / 10;
            try {
                analyzeTable(session, table, rows, false);
            } finally {
                analyzing.set(false);
            }
        }
    }

    /**
     * Analyze this table.
     *
     * @param session the session
     * @param table the table
     * @param sample the number of sample rows
     * @param manual whether the command was called by the user
     */
    public static void analyzeTable(ServerSession session, Table table, int sample, boolean manual) {
        if (table.getTableType() != TableType.STANDARD_TABLE || table.isHidden() || session == null) {
            return;
        }
        if (!manual) {
            if (session.getDatabase().isSysTableLocked()) {
                return;
            }
            if (table.hasSelectTrigger()) {
                return;
            }
        }
        if (table.isTemporary() && !table.isGlobalTemporary()
                && session.findLocalTempTable(table.getName()) == null) {
            return;
        }
        if (table.isLockedExclusively() && !table.isLockedExclusivelyBy(session)) {
            return;
        }
        if (!session.getUser().hasRight(table, Right.SELECT)) {
            return;
        }
        if (session.getCancel() != 0) {
            // if the connection is closed and there is something to undo
            return;
        }
        Column[] columns = table.getColumns();
        if (columns.length == 0) {
            return;
        }
        Database db = session.getDatabase();
        StatementBuilder buff = new StatementBuilder("SELECT ");
        for (Column col : columns) {
            buff.appendExceptFirst(", ");
            int type = col.getType();
            if (type == Value.BLOB || type == Value.CLOB) {
                // can not index LOB columns, so calculating
                // the selectivity is not required
                buff.append("MAX(NULL)");
            } else {
                buff.append("SELECTIVITY(").append(col.getSQL()).append(')');
            }
        }
        buff.append(" FROM ").append(table.getSQL());
        if (sample > 0) {
            buff.append(" LIMIT ? SAMPLE_SIZE ? ");
        }
        String sql = buff.toString();
        PreparedSQLStatement command = session.prepareStatement(sql);
        if (sample > 0) {
            List<? extends CommandParameter> params = command.getParameters();
            params.get(0).setValue(ValueInt.get(1));
            params.get(1).setValue(ValueInt.get(sample));
        }
        Result result = command.query(0);
        result.next();
        for (int j = 0; j < columns.length; j++) {
            Value v = result.currentRow()[j];
            if (v != ValueNull.INSTANCE) {
                int selectivity = v.getInt();
                columns[j].setSelectivity(selectivity);
            }
        }
        if (manual) {
            db.updateMeta(session, table);
        } else {
            ServerSession sysSession = db.getSystemSession();
            if (sysSession != session) {
                // if the current session is the system session
                // (which is the case if we are within a trigger)
                // then we can't update the statistics because
                // that would unlock all locked objects
                synchronized (sysSession) {
                    synchronized (db) {
                        db.updateMeta(sysSession, table);
                        sysSession.commit();
                    }
                }
            }
        }
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.table;

import java.util.concurrent.atomic.AtomicBoolean;

import com.lealone.common.util.StatementBuilder;
import com.lealone.db.auth.Right;
import com.lealone.db.result.Result;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.PreparedSQLStatement;

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
    public void analyzeIfRequired(ServerSession session) {
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
                Scheduler scheduler = session.getScheduler();
                if (scheduler != null) {
                    scheduler.handle(() -> {
                        // 如果有多个线程要执行analyze，只需要其中一个执行即可
                        if (analyzing.compareAndSet(false, true)) {
                            try (ServerSession s = session.getDatabase()
                                    .createSession(session.getUser())) {
                                analyzeTable(s, table, rows, false);
                                s.asyncCommit();
                            } finally {
                                analyzing.set(false);
                            }
                        }
                    });
                }
            } finally {
                analyzing.set(false);
            }
        }
    }

    // 通过手工执行ANALYZE语句调用
    public void analyze(ServerSession session, int sample) {
        if (analyzing.compareAndSet(false, true)) {
            try {
                analyzeTable(session, table, sample, true);
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
    private void analyzeTable(ServerSession session, Table table, int sample, boolean manual) {
        if (table.getTableType() != TableType.STANDARD_TABLE || table.isHidden() || session == null) {
            return;
        }
        if (!manual && table.hasSelectTrigger()) {
            return;
        }
        if (table.isTemporary() && !table.isGlobalTemporary()
                && session.findLocalTempTable(table.getName()) == null) {
            return;
        }
        if (!session.getUser().hasRight(table, Right.SELECT)) {
            return;
        }
        // if the connection is closed and there is something to undo
        if (session.getCancel() != 0) {
            return;
        }
        Column[] columns = table.getColumns();
        if (columns.length == 0) {
            return;
        }
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
            buff.append(" LIMIT 1 SAMPLE_SIZE ").append(sample);
        }
        String sql = buff.toString();
        analyzeTable(session, table, sql);
    }

    private static void analyzeTable(ServerSession session, Table table, String sql) {
        // 执行updateMeta时会修改modificationMetaID，所以queryCache不起作用
        PreparedSQLStatement command = session.prepareStatement(sql);
        Column[] columns = table.getColumns();
        Result result = command.query(0);
        result.next();
        for (int i = 0; i < columns.length; i++) {
            Value v = result.currentRow()[i];
            if (v != ValueNull.INSTANCE) {
                int selectivity = v.getInt();
                columns[i].setSelectivity(selectivity);
            }
        }
        session.getDatabase().updateMeta(session, table);
        // session.asyncCommit(); //不能立刻调用，要由上层提交
    }
}

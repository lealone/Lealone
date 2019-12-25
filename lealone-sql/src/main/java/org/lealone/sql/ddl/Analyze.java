/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import java.util.ArrayList;

import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Database;
import org.lealone.db.auth.Right;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.expression.Parameter;

/**
 * This class represents the statement
 * ANALYZE
 */
public class Analyze extends DefinitionStatement {

    /**
     * The sample size.
     */
    private int sampleRows;

    public Analyze(ServerSession session) {
        super(session);
        sampleRows = session.getDatabase().getSettings().analyzeSample;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        for (Table table : db.getAllTablesAndViews(false)) {
            analyzeTable(session, table, sampleRows, true);
        }
        return 0;
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
        if (table.isTemporary() && !table.isGlobalTemporary() && session.findLocalTempTable(table.getName()) == null) {
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
        StatementBase command = (StatementBase) session.prepareStatement(sql);
        if (sample > 0) {
            ArrayList<Parameter> params = command.getParameters();
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

    public void setTop(int top) {
        this.sampleRows = top;
    }

    @Override
    public int getType() {
        return SQLStatement.ANALYZE;
    }

}

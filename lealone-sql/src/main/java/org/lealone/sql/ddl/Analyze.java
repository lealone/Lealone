/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableAnalyzer;
import org.lealone.sql.SQLStatement;

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
    public int getType() {
        return SQLStatement.ANALYZE;
    }

    public void setTop(int top) {
        this.sampleRows = top;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        for (Table table : db.getAllTablesAndViews(false)) {
            TableAnalyzer.analyzeTable(session, table, sampleRows, true);
        }
        return 0;
    }
}

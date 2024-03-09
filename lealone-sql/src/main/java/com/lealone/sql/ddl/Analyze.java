/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.db.Database;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * ANALYZE
 */
public class Analyze extends DefinitionStatement {

    /**
     * The sample size.
     */
    private int sample;

    public Analyze(ServerSession session) {
        super(session);
        sample = session.getDatabase().getSettings().analyzeSample;
    }

    @Override
    public int getType() {
        return SQLStatement.ANALYZE;
    }

    public void setSample(int sample) {
        this.sample = sample;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        for (Table table : db.getAllTablesAndViews(false)) {
            table.analyze(session, sample);
        }
        return 0;
    }
}

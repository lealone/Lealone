/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql;

import java.util.ArrayList;

import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.expression.Parameter;

/**
 * Represents a list of SQL statements.
 * 
 * @author H2 Group
 * @author zhh
 */
public class StatementList extends StatementBase {

    private final StatementBase firstStatement;
    private final String remaining;

    public StatementList(ServerSession session, StatementBase firstStatement, String remaining) {
        super(session);
        this.firstStatement = firstStatement;
        this.remaining = remaining;
    }

    @Override
    public int getType() {
        return firstStatement.getType();
    }

    @Override
    public Result getMetaData() {
        return firstStatement.getMetaData();
    }

    @Override
    public ArrayList<Parameter> getParameters() {
        return firstStatement.getParameters();
    }

    private void executeRemaining() {
        StatementBase remainingStatement = (StatementBase) session.prepareStatement(remaining, -1);
        if (remainingStatement.isQuery()) {
            remainingStatement.query(0);
        } else {
            remainingStatement.update();
        }
    }

    @Override
    public boolean isQuery() {
        return firstStatement.isQuery();
    }

    @Override
    public Result query(int maxRows) {
        Result result = firstStatement.query(maxRows);
        executeRemaining();
        return result;
    }

    @Override
    public int update() {
        int updateCount = firstStatement.update();
        executeRemaining();
        return updateCount;
    }
}

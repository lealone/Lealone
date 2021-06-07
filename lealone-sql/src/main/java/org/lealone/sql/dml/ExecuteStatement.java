/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.dml;

import java.util.ArrayList;

import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;

public abstract class ExecuteStatement extends ManipulationStatement {

    protected final ArrayList<Expression> expressions = new ArrayList<>();

    public ExecuteStatement(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.EXECUTE;
    }

    /**
     * Set the expression at the given index.
     *
     * @param index the index (0 based)
     * @param expr the expression
     */
    public void setExpression(int index, Expression expr) {
        expressions.add(index, expr);
    }
}

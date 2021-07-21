/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.evaluator;

import org.lealone.db.session.ServerSession;
import org.lealone.sql.expression.Expression;

//解释执行表达式
public class ExpressionInterpreter implements ExpressionEvaluator {

    private final ServerSession session;
    private final Expression expression;

    public ExpressionInterpreter(ServerSession session, Expression expression) {
        this.session = session;
        this.expression = expression;
    }

    @Override
    public boolean getBooleanValue() {
        return expression.getBooleanValue(session);
    }
}

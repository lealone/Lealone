/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.evaluator;

import org.lealone.db.session.ServerSession;
import org.lealone.sql.expression.Expression;

//默认先解释执行，一旦发现是热点就采用编译执行
public class HotSpotEvaluator implements ExpressionEvaluator {

    private final ServerSession session;
    private final Expression expression;

    private ExpressionEvaluator evaluator;
    private int count;
    private boolean isJit;

    public HotSpotEvaluator(ServerSession session, Expression expression) {
        this.session = session;
        this.expression = expression;
        evaluator = new ExpressionInterpreter(session, expression);
    }

    @Override
    public boolean getBooleanValue() {
        if (!isJit && ++count > 1000) { // TODO 允许配置
            isJit = true;
            ExpressionCompiler.createJitEvaluatorAsync(session, expression, ar -> {
                if (ar.isSucceeded()) {
                    evaluator = ar.getResult();
                }
            });
        }
        return evaluator.getBooleanValue();
    }
}

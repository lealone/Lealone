/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.evaluator;

import java.util.ArrayList;

import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;

//默认先解释执行，一旦发现是热点就采用编译执行
public class HotSpotEvaluator implements ExpressionEvaluator {

    private final ArrayList<ExpressionColumn> expressionColumnList = new ArrayList<>();
    private final ArrayList<Value> valueList = new ArrayList<>();

    private final ServerSession session;
    private final Expression expression;

    private ExpressionEvaluator evaluator;
    private int count;
    private boolean isJit;
    private boolean async;

    public HotSpotEvaluator(ServerSession session, Expression expression) {
        this(session, expression, true);
    }

    public HotSpotEvaluator(ServerSession session, Expression expression, boolean async) {
        this.session = session;
        this.expression = expression;
        this.async = async;
        evaluator = new ExpressionInterpreter(session, expression);
    }

    public void addExpressionColumn(ExpressionColumn ec) {
        expressionColumnList.add(ec);
    }

    public ExpressionColumn getExpressionColumn(int index) {
        return expressionColumnList.get(index);
    }

    public int getExpressionColumnListSize() {
        return expressionColumnList.size();
    }

    public void addValue(Value v) {
        valueList.add(v);
    }

    public Value getValue(int index) {
        return valueList.get(index);
    }

    public int getValueListSize() {
        return valueList.size();
    }

    @Override
    public boolean getBooleanValue() {
        if (!isJit && ++count > 1000) { // TODO 允许配置
            isJit = true;
            if (async) {
                ExpressionCompiler.createJitEvaluatorAsync(this, session, expression, ar -> {
                    if (ar.isSucceeded()) {
                        setEvaluator(ar.getResult());
                    }
                });
            } else {
                JitEvaluator e = ExpressionCompiler.createJitEvaluator(this, session, expression);
                setEvaluator(e);
            }
        }
        return evaluator.getBooleanValue();
    }

    private void setEvaluator(JitEvaluator e) {
        e.setHotSpotEvaluator(HotSpotEvaluator.this);
        e.setSession(session);
        evaluator = e;
    }
}

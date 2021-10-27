/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.evaluator;

import java.util.ArrayList;
import java.util.HashSet;

import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;

//默认先解释执行，一旦发现是热点就采用编译执行
public class HotSpotEvaluator implements ExpressionEvaluator {

    private final ArrayList<Expression> expressionList = new ArrayList<>();
    private final ArrayList<ExpressionColumn> expressionColumnList = new ArrayList<>();
    private final ArrayList<Value> valueList = new ArrayList<>();

    private final ServerSession session;
    private final Expression expression;

    private ExpressionEvaluator evaluator;
    private int count;
    private boolean isJit;
    private boolean async;
    private int expressionCompileThreshold;

    // 用于支持动态编译ConditionInConstantSet表达式
    private HashSet<Value> valueSet;

    public HashSet<Value> getValueSet() {
        return valueSet;
    }

    public void setValueSet(HashSet<Value> valueSet) {
        this.valueSet = valueSet;
    }

    public HotSpotEvaluator(ServerSession session, Expression expression) {
        this(session, expression, true);
    }

    public HotSpotEvaluator(ServerSession session, Expression expression, boolean async) {
        this.session = session;
        this.expression = expression;
        this.async = async;
        this.expressionCompileThreshold = session.getExpressionCompileThreshold();
        evaluator = new ExpressionInterpreter(session, expression);
    }

    public ServerSession getSession() {
        return session;
    }

    public void addExpression(Expression e) {
        expressionList.add(e);
    }

    public Expression getExpression(int index) {
        return expressionList.get(index);
    }

    public int getExpressionListSize() {
        return expressionList.size();
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
        if (!isJit && expressionCompileThreshold > 0 && count++ > expressionCompileThreshold) {
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
        e.setHotSpotEvaluator(this);
        e.setSession(session);
        evaluator = e;
    }
}

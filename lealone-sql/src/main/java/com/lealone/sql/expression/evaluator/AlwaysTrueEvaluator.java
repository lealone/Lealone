/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.evaluator;

//比如没有where条件时就用这个类
public class AlwaysTrueEvaluator implements ExpressionEvaluator {

    @Override
    public boolean getBooleanValue() {
        return true;
    }

}

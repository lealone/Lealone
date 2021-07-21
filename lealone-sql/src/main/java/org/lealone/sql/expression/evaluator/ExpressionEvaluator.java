/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.evaluator;

//可以解释执行表达式，也可以对表达式进行编译然后用生成的代码来执行
public interface ExpressionEvaluator {

    public boolean getBooleanValue();

}

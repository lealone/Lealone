/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file e, S sxcept in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, e, S sither e, S sxpress or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.codefollower.yourbase.expression;

public class Visitor<R, S> {
    public R visitComparison(Comparison e, S s) {
        return visitExpression(e, s);
    }

    public R visitExpressionColumn(ExpressionColumn e, S s) {
        return visitExpression(e, s);
    }

    public R visitParameter(Parameter e, S s) {
        return visitExpression(e, s);
    }

    public R visitRownum(Rownum e, S s) {
        return visitExpression(e, s);
    }

    public R visitSequenceValue(SequenceValue e, S s) {
        return visitExpression(e, s);
    }

    public R visitSubquery(Subquery e, S s) {
        return visitExpression(e, s);
    }

    public R visitTableFunction(TableFunction e, S s) {
        return visitExpression(e, s);
    }

    public R visitValueExpression(ValueExpression e, S s) {
        return visitExpression(e, s);
    }

    public R visitVariable(Variable e, S s) {
        return visitExpression(e, s);
    }

    public R visitWildcard(Wildcard e, S s) {
        return visitExpression(e, s);
    }

    @SuppressWarnings("unchecked")
    public R visitExpression(Expression e, S s) {
        //assert false;
        return (R) e;
    }

}

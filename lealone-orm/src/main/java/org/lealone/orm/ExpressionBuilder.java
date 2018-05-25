/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.orm;

import java.util.ArrayList;
import java.util.Collection;

import org.lealone.common.util.New;
import org.lealone.db.result.SelectOrderBy;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.sql.expression.CompareLike;
import org.lealone.sql.expression.Comparison;
import org.lealone.sql.expression.ConditionAndOr;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ValueExpression;

public class ExpressionBuilder<T> {

    private Model<?> model;
    private Model<?> oldModel;
    private Expression expression;
    private ArrayList<SelectOrderBy> orderList;
    private boolean isAnd = true;

    ExpressionBuilder(Model<?> model) {
        this.model = this.oldModel = model;
    }

    void setAnd(boolean isAnd) {
        this.isAnd = isAnd;
    }

    // 用于join时切换
    void setModel(Model<?> model) {
        this.oldModel = this.model;
        this.model = model;
    }

    Model<?> getModel() {
        return model;
    }

    Model<?> getOldModel() {
        return oldModel;
    }

    Expression getExpression() {
        return expression;
    }

    ExpressionBuilder<T> junction(ExpressionBuilder<T> e) {
        setRootExpression(e.getExpression());
        return this;
    }

    ArrayList<SelectOrderBy> getOrderList() {
        return orderList;
    }

    private ModelTable getTable() {
        return model.getTable();
    }

    private Comparison createComparison(String propertyName, Object value, int compareType) {
        ExpressionColumn ec = model.getExpressionColumn(propertyName);
        ValueExpression v;
        if (value instanceof Value) {
            v = ValueExpression.get((Value) value);
        } else {
            v = ValueExpression.get(ValueString.get(value.toString()));
        }
        return new Comparison(getTable().getSession(), compareType, ec, v);
    }

    private ConditionAndOr createConditionAnd(Expression left, Expression right) {
        return new ConditionAndOr(isAnd ? ConditionAndOr.AND : ConditionAndOr.OR, left, right);
    }

    private void setRootExpression(Expression e) {
        if (expression == null) {
            expression = e;
        } else {
            expression = createConditionAnd(expression, e);
        }
    }

    private void setRootExpression(String propertyName, Object value, int compareType) {
        Comparison c = createComparison(propertyName, value, compareType);
        setRootExpression(c);
    }

    public ExpressionBuilder<T> set(String propertyName, Value value) {
        model.addNVPair(propertyName, value);
        return this;
    }

    public ExpressionBuilder<T> eq(String propertyName, ModelProperty<?> p) {
        ExpressionColumn left = model.getExpressionColumn(propertyName);
        ExpressionColumn right = Model.getExpressionColumn(p);
        Comparison c = new Comparison(getTable().getSession(), Comparison.EQUAL, left, right);
        setRootExpression(c);
        return this;
    }

    public ExpressionBuilder<T> eq(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.EQUAL);
        return this;
    }

    public ExpressionBuilder<T> ne(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.NOT_EQUAL);
        return this;
    }

    public ExpressionBuilder<T> ieq(String propertyName, String value) {
        setRootExpression(propertyName, value, Comparison.EQUAL);
        return this;
    }

    public ExpressionBuilder<T> between(String propertyName, Object value1, Object value2) {
        setRootExpression(propertyName, value1, Comparison.BIGGER_EQUAL);
        setRootExpression(propertyName, value2, Comparison.SMALLER_EQUAL);
        return this;
    }

    public ExpressionBuilder<T> gt(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.BIGGER);
        return this;
    }

    public ExpressionBuilder<T> ge(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.BIGGER_EQUAL);
        return this;
    }

    public ExpressionBuilder<T> lt(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.SMALLER);
        return this;
    }

    public ExpressionBuilder<T> le(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.SMALLER_EQUAL);
        return this;
    }

    public ExpressionBuilder<T> isNull(String propertyName) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> isNotNull(String propertyName) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> arrayContains(String propertyName, Object... values) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> arrayNotContains(String propertyName, Object... values) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> arrayIsEmpty(String propertyName) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> arrayIsNotEmpty(String propertyName) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> in(String propertyName, Object... values) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> in(String propertyName, Collection<?> values) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> notIn(String propertyName, Object... values) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> notIn(String propertyName, Collection<?> values) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> like(String propertyName, String value) {
        ExpressionColumn ec = model.getExpressionColumn(propertyName);
        ValueExpression v = ValueExpression.get(ValueString.get(value));
        CompareLike like = new CompareLike(getTable().getDatabase(), ec, v, null, false);
        setRootExpression(like);
        return this;
    }

    public ExpressionBuilder<T> ilike(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> startsWith(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> istartsWith(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> endsWith(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> iendsWith(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> contains(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> icontains(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> match(String propertyName, String search) {
        // TODO Auto-generated method stub
        return this;
    }

    public ExpressionBuilder<T> and() {
        isAnd = true;
        return this;
    }

    public ExpressionBuilder<T> or() {
        isAnd = false;
        return this;
    }

    public ExpressionBuilder<T> not() {
        // TODO Auto-generated method stub
        return null;
    }

    public ExpressionBuilder<T> orderBy(String propertyName, boolean isDesc) {
        if (orderList == null)
            orderList = New.arrayList();
        SelectOrderBy order = new SelectOrderBy();
        order.expression = model.getExpressionColumn(propertyName);
        order.descending = isDesc;
        orderList.add(order);
        return this;
    }

}

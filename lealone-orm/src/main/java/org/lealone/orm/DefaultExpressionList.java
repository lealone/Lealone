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

public class DefaultExpressionList<T> implements ExpressionList<T> {

    org.lealone.db.table.Table dbTable;
    Table table;
    org.lealone.sql.expression.Expression expression;
    Query<?> query;

    boolean isAnd = true;

    ArrayList<SelectOrderBy> orderList = New.arrayList();

    void setAnd(boolean isAnd) {
        this.isAnd = isAnd;
    }

    @Override
    public org.lealone.sql.expression.Expression getExpression() {
        return expression;
    }

    public DefaultExpressionList(Query<?> query, Table table) {
        this.table = table;
        if (table != null)
            this.dbTable = table.getDbTable();
        this.query = query;
    }

    Table getTable() {
        table = query.getTable();
        dbTable = table.getDbTable();
        return table;
    }

    private ExpressionColumn getExpressionColumn(String propertyName) {
        getTable();
        return new ExpressionColumn(dbTable.getDatabase(), dbTable.getSchema().getName(), dbTable.getName(),
                propertyName);
        // return new ExpressionColumn(dbTable.getDatabase(), dbTable.getColumn(propertyName));
    }

    private Comparison createComparison(String propertyName, Object value, int compareType) {
        ExpressionColumn ec = getExpressionColumn(propertyName);
        ValueExpression v;
        if (value instanceof Value) {
            v = ValueExpression.get((Value) value);
        } else {
            v = ValueExpression.get(ValueString.get(value.toString()));
        }
        return new Comparison(table.getSession(), compareType, ec, v);
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

    // private Comparison createComparison(String propertyName, Value value, int compareType) {
    // ExpressionColumn ec = getExpressionColumn(propertyName);
    // ValueExpression v = ValueExpression.get(value);
    // return new Comparison(table.getSession(), compareType, ec, v);
    // }
    //
    // private void setRootExpression(String propertyName, Value value, int compareType) {
    // Comparison c = createComparison(propertyName, value, compareType);
    // setRootExpression(c);
    // }

    @Override
    public ExpressionList<T> set(String propertyName, Value value) {
        query.addNVPair(propertyName, value);
        return this;
    }

    @Override
    public ExpressionList<T> eq(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.EQUAL);
        return this;
    }

    @Override
    public ExpressionList<T> ne(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.NOT_EQUAL);
        return this;
    }

    @Override
    public ExpressionList<T> ieq(String propertyName, String value) {
        setRootExpression(propertyName, value, Comparison.EQUAL);
        return this;
    }

    @Override
    public ExpressionList<T> between(String propertyName, Object value1, Object value2) {
        setRootExpression(propertyName, value1, Comparison.BIGGER_EQUAL);
        setRootExpression(propertyName, value2, Comparison.SMALLER_EQUAL);
        return this;
    }

    @Override
    public ExpressionList<T> betweenProperties(String lowProperty, String highProperty, Object value) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> gt(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.BIGGER);
        return this;
    }

    @Override
    public ExpressionList<T> ge(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.BIGGER_EQUAL);
        return this;
    }

    @Override
    public ExpressionList<T> lt(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.SMALLER);
        return this;
    }

    @Override
    public ExpressionList<T> le(String propertyName, Object value) {
        setRootExpression(propertyName, value, Comparison.SMALLER_EQUAL);
        return this;
    }

    @Override
    public ExpressionList<T> isNull(String propertyName) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> isNotNull(String propertyName) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> arrayContains(String propertyName, Object... values) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> arrayNotContains(String propertyName, Object... values) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> arrayIsEmpty(String propertyName) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> arrayIsNotEmpty(String propertyName) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> in(String propertyName, Object... values) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> in(String propertyName, Collection<?> values) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> notIn(String propertyName, Object... values) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> notIn(String propertyName, Collection<?> values) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> like(String propertyName, String value) {
        ExpressionColumn ec = getExpressionColumn(propertyName);
        ValueExpression v = ValueExpression.get(ValueString.get(value));
        CompareLike like = new CompareLike(dbTable.getDatabase(), ec, v, null, false);
        setRootExpression(like);
        return this;
    }

    @Override
    public ExpressionList<T> ilike(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> startsWith(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> istartsWith(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> endsWith(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> iendsWith(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> contains(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> icontains(String propertyName, String value) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> match(String propertyName, String search) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public ExpressionList<T> and() {
        isAnd = true;
        return this;
    }

    @Override
    public ExpressionList<T> or() {
        isAnd = false;
        return this;
    }

    @Override
    public ExpressionList<T> not() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ExpressionList<T> orderBy(String propertyName, boolean isDesc) {
        SelectOrderBy order = new SelectOrderBy();
        order.expression = getExpressionColumn(propertyName);
        order.descending = isDesc;
        orderList.add(order);
        return this;
    }

    @Override
    public ExpressionList<T> junction(ExpressionList<T> e) {
        setRootExpression(e.getExpression());
        return this;
    }

    @Override
    public ArrayList<SelectOrderBy> getOrderList() {
        return orderList;
    }

}

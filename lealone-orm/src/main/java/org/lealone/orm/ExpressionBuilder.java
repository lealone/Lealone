/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm;

import java.util.ArrayList;
import java.util.Collection;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueBoolean;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.SelectOrderBy;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.condition.CompareLike;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.expression.condition.ConditionAndOr;
import org.lealone.sql.expression.condition.ConditionIn;
import org.lealone.sql.expression.condition.ConditionNot;
import org.lealone.sql.expression.function.Function;

public class ExpressionBuilder<T> {

    private Model<?> model;
    private Model<?> oldModel;
    private Expression expression;
    private ArrayList<SelectOrderBy> orderList;
    private boolean isAnd = true;
    private boolean isNot;

    ExpressionBuilder(Model<?> model) {
        this.model = this.oldModel = model;
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

    private ModelTable getModelTable() {
        return model.getModelTable();
    }

    private ValueExpression createValueExpression(Object value) {
        ValueExpression v;
        if (value instanceof Value) {
            v = ValueExpression.get((Value) value);
        } else {
            v = ValueExpression.get(ValueString.get(value.toString()));
        }
        return v;
    }

    private ArrayList<Expression> createExpressionList(Object... values) {
        ArrayList<Expression> list = new ArrayList<>(values.length);
        for (Object v : values)
            list.add(createValueExpression(v));
        return list;
    }

    private Comparison createComparison(String propertyName, Object value, int compareType) {
        ExpressionColumn ec = model.getExpressionColumn(propertyName);
        ValueExpression v = createValueExpression(value);
        return new Comparison(getModelTable().getSession(), compareType, ec, v);
    }

    private ConditionAndOr createConditionAnd(Expression left, Expression right) {
        return new ConditionAndOr(isAnd ? ConditionAndOr.AND : ConditionAndOr.OR, left, right);
    }

    private void setRootExpression(Expression e) {
        if (isNot) {
            e = new ConditionNot(e);
            isNot = false;
        }
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
        Comparison c = new Comparison(getModelTable().getSession(), Comparison.EQUAL, left, right);
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
        Expression left = createExpressionColumn(propertyName, true);
        value = value.toUpperCase();
        ValueExpression v = ValueExpression.get(ValueString.get(value));
        Comparison c = new Comparison(getModelTable().getSession(), Comparison.EQUAL, left, v);
        setRootExpression(c);
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
        setRootExpression(propertyName, ValueNull.INSTANCE, Comparison.IS_NULL);
        return this;
    }

    public ExpressionBuilder<T> isNotNull(String propertyName) {
        setRootExpression(propertyName, ValueNull.INSTANCE, Comparison.IS_NOT_NULL);
        return this;
    }

    private void arrayComparison(String propertyName, boolean contains, Object... values) {
        ExpressionColumn ec = model.getExpressionColumn(propertyName);
        Function f = Function.getFunction(getModelTable().getDatabase(), "ARRAY_CONTAINS");
        f.setParameter(0, ec);

        Value[] array = new Value[values.length];
        for (int i = 0; i < values.length; i++) {
            array[i] = ValueString.get(values[i].toString());
        }
        ValueExpression v = ValueExpression.get(ValueArray.get(array));
        f.setParameter(1, v);

        Comparison c = new Comparison(getModelTable().getSession(), Comparison.EQUAL, f,
                ValueExpression.get(contains ? ValueBoolean.TRUE : ValueBoolean.FALSE));
        setRootExpression(c);
    }

    public ExpressionBuilder<T> arrayContains(String propertyName, Object... values) {
        arrayComparison(propertyName, true, values);
        return this;
    }

    public ExpressionBuilder<T> arrayNotContains(String propertyName, Object... values) {
        arrayComparison(propertyName, false, values);
        return this;
    }

    public ExpressionBuilder<T> arrayIsEmpty(String propertyName) {
        return arrayLength(propertyName, Comparison.EQUAL);
    }

    public ExpressionBuilder<T> arrayIsNotEmpty(String propertyName) {
        return arrayLength(propertyName, Comparison.BIGGER);
    }

    private ExpressionBuilder<T> arrayLength(String propertyName, int compareType) {
        ExpressionColumn ec = model.getExpressionColumn(propertyName);
        Function f = Function.getFunction(getModelTable().getDatabase(), "ARRAY_LENGTH");
        f.setParameter(0, ec);
        ValueExpression v = ValueExpression.get(ValueInt.get(0));
        Comparison c = new Comparison(getModelTable().getSession(), compareType, f, v);
        setRootExpression(c);
        return this;
    }

    private ConditionIn createConditionIn(String propertyName, Object... values) {
        ExpressionColumn left = model.getExpressionColumn(propertyName);
        ArrayList<Expression> valueList = createExpressionList(values);
        ConditionIn c = new ConditionIn(getModelTable().getDatabase(), left, valueList);
        return c;
    }

    public ExpressionBuilder<T> in(String propertyName, Object... values) {
        ConditionIn c = createConditionIn(propertyName, values);
        setRootExpression(c);
        return this;
    }

    public ExpressionBuilder<T> in(String propertyName, Collection<?> values) {
        Object[] valueArray = new Object[values.size()];
        values.toArray(valueArray);
        in(propertyName, valueArray);
        return this;
    }

    public ExpressionBuilder<T> notIn(String propertyName, Object... values) {
        ConditionIn c = createConditionIn(propertyName, values);
        setRootExpression(new ConditionNot(c));
        return this;
    }

    public ExpressionBuilder<T> notIn(String propertyName, Collection<?> values) {
        Object[] valueArray = new Object[values.size()];
        values.toArray(valueArray);
        notIn(propertyName, valueArray);
        return this;
    }

    private ExpressionBuilder<T> like(String propertyName, String value, boolean caseInsensitive) {
        return like(propertyName, value, caseInsensitive, false);
    }

    private Expression createExpressionColumn(String propertyName, boolean caseInsensitive) {
        ExpressionColumn ec = model.getExpressionColumn(propertyName);
        if (!caseInsensitive)
            return ec;
        Function f = Function.getFunction(getModelTable().getDatabase(), "UPPER");
        f.setParameter(0, ec);
        return f;
    }

    private ExpressionBuilder<T> like(String propertyName, String value, boolean caseInsensitive, boolean regexp) {
        Expression left = createExpressionColumn(propertyName, caseInsensitive);
        if (caseInsensitive) {
            value = value.toUpperCase();
        }
        ValueExpression v = ValueExpression.get(ValueString.get(value));
        CompareLike like = new CompareLike(getModelTable().getDatabase(), left, v, null, regexp);
        setRootExpression(like);
        return this;
    }

    public ExpressionBuilder<T> like(String propertyName, String value) {
        return like(propertyName, value, false);
    }

    public ExpressionBuilder<T> ilike(String propertyName, String value) {
        return like(propertyName, value, true);
    }

    public ExpressionBuilder<T> startsWith(String propertyName, String value) {
        value = value + "%";
        return like(propertyName, value, false);
    }

    public ExpressionBuilder<T> istartsWith(String propertyName, String value) {
        value = value + "%";
        return like(propertyName, value, true);
    }

    public ExpressionBuilder<T> endsWith(String propertyName, String value) {
        value = "%" + value;
        return like(propertyName, value, false);
    }

    public ExpressionBuilder<T> iendsWith(String propertyName, String value) {
        value = "%" + value;
        return like(propertyName, value, true);
    }

    public ExpressionBuilder<T> contains(String propertyName, String value) {
        value = "%" + value + "%";
        return like(propertyName, value, false);
    }

    public ExpressionBuilder<T> icontains(String propertyName, String value) {
        value = "%" + value + "%";
        return like(propertyName, value, true);
    }

    public ExpressionBuilder<T> match(String propertyName, String search) {
        return like(propertyName, search, false, true);
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
        isNot = !isNot; // 两次not相当于无
        return this;
    }

    public ExpressionBuilder<T> orderBy(String propertyName, boolean isDesc) {
        if (orderList == null)
            orderList = new ArrayList<>();
        SelectOrderBy order = new SelectOrderBy();
        order.expression = model.getExpressionColumn(propertyName);
        order.descending = isDesc;
        orderList.add(order);
        return this;
    }

}

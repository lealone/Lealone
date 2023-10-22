/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.operator;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.expression.condition.ConditionAndOr;
import org.lealone.sql.expression.condition.ConditionInConstantSet;
import org.lealone.sql.expression.condition.ConditionNot;
import org.lealone.sql.optimizer.TableFilter;

public class BOQueryOperator extends BsonOperator {

    public static Expression toWhereCondition(BsonDocument doc, TableFilter tableFilter,
            ServerSession session) {
        Expression condition = null;
        for (Entry<String, BsonValue> e : doc.entrySet()) {
            String k = e.getKey();
            BsonValue v = e.getValue();
            if (k.charAt(0) == '$') {
                switch (k) {
                case "$and":
                    return toAndOrExpression(v.asArray(), tableFilter, session, ConditionAndOr.AND);
                case "$or":
                    return toAndOrExpression(v.asArray(), tableFilter, session, ConditionAndOr.OR);
                case "$nor":
                    return new ConditionNot(
                            toAndOrExpression(v.asArray(), tableFilter, session, ConditionAndOr.OR));
                case "$not":
                    return new ConditionNot(toWhereCondition(v.asDocument(), tableFilter, session));
                case "$expr":
                    return BOAggregateOperator.toAggregateExpression(v.asDocument(), tableFilter,
                            session);
                default:
                    throw DbException.getUnsupportedException("query operator " + k);
                }
            } else {
                String columnName = k.toUpperCase();
                if ("_ID".equals(columnName)) {
                    columnName = Column.ROWID;
                }
                Expression left = getExpressionColumn(tableFilter, columnName);
                Expression right = null;
                Expression cond = null;
                if (v.isDocument()) {
                    right = toColumnValueExpression(v.asDocument(), tableFilter, session, left);
                    cond = right;
                } else {
                    right = toValueExpression(e.getValue());
                    cond = new Comparison(session, Comparison.EQUAL, left, right);
                }
                if (condition == null) {
                    condition = cond;
                } else {
                    condition = new ConditionAndOr(ConditionAndOr.AND, cond, condition);
                }
            }
        }
        return condition;
    }

    public static Expression toAndOrExpression(BsonArray ba, TableFilter tableFilter,
            ServerSession session, int andOrType) {
        Expression e = null;
        for (int i = 0, size = ba.size(); i < size; i++) {
            Expression cond = toWhereCondition(ba.get(i).asDocument(), tableFilter, session);
            if (e == null) {
                e = cond;
            } else {
                e = new ConditionAndOr(andOrType, cond, e);
            }
        }
        return e;
    }

    public static Expression toColumnValueExpression(BsonDocument doc, TableFilter tableFilter,
            ServerSession session, Expression left) {
        Expression ret = null;
        for (Entry<String, BsonValue> e : doc.entrySet()) {
            String k = e.getKey();
            BsonValue v = e.getValue();
            Expression c = toColumnValueExpression(k, v, tableFilter, session, left);
            if (ret == null) {
                ret = c;
            } else {
                ret = new ConditionAndOr(ConditionAndOr.AND, c, ret);
            }
        }
        return ret;
    }

    public static Expression toColumnValueExpression(String k, BsonValue v, TableFilter tableFilter,
            ServerSession session, Expression left) {
        if (k.charAt(0) == '$') {
            switch (k) {
            case "$eq":
                return new Comparison(session, Comparison.EQUAL, left, toValueExpression(v));
            case "$ne":
                return new Comparison(session, Comparison.NOT_EQUAL, left, toValueExpression(v));
            case "$gt":
                return new Comparison(session, Comparison.BIGGER, left, toValueExpression(v));
            case "$gte":
                return new Comparison(session, Comparison.BIGGER_EQUAL, left, toValueExpression(v));
            case "$lt":
                return new Comparison(session, Comparison.SMALLER, left, toValueExpression(v));
            case "$lte":
                return new Comparison(session, Comparison.SMALLER_EQUAL, left, toValueExpression(v));
            case "$in":
                return toInConstantSet(v.asArray(), tableFilter, session, left);
            case "$nin":
                return new ConditionNot(toInConstantSet(v.asArray(), tableFilter, session, left));
            case "$exists":
                if (v.asBoolean().getValue())
                    return new Comparison(session, Comparison.IS_NOT_NULL, left, null);
                else
                    return new Comparison(session, Comparison.IS_NULL, left, null);
            case "$not":
                return new ConditionNot(
                        toColumnValueExpression(v.asDocument(), tableFilter, session, left));
            default:
                throw DbException.getUnsupportedException("query operator " + k);
            }
        } else {
            throw DbException.getUnsupportedException("query operator " + k);
        }
    }

    public static Expression toInConstantSet(BsonArray ba, TableFilter tableFilter,
            ServerSession session, Expression left) {
        int size = ba.size();
        ArrayList<Expression> valueList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            valueList.add(toValueExpression(ba.get(i)));
        }
        // 需要提前知道left的类型
        left.mapColumns(tableFilter, 0);
        left.optimize(session);
        return new ConditionInConstantSet(session, left, valueList);
    }
}

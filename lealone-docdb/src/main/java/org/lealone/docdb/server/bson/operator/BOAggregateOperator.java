/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.bson.operator;

import java.util.Map.Entry;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.expression.condition.ConditionAndOr;
import org.lealone.sql.expression.condition.ConditionNot;
import org.lealone.sql.optimizer.TableFilter;

public class BOAggregateOperator extends BOQueryOperator {

    // Convert Aggregation Pipeline Operators To SQL Expression
    public static Expression convertApoToSqlExpression(BsonDocument doc, TableFilter tableFilter,
            ServerSession session) {
        return null;
    }

    public static Expression toAggregateExpression(BsonDocument doc, TableFilter tableFilter,
            ServerSession session) {
        Expression condition = null;
        for (Entry<String, BsonValue> e : doc.entrySet()) {
            String k = e.getKey();
            BsonValue v = e.getValue();
            BsonArray ba = null;
            if (v.isArray())
                ba = v.asArray();
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
                    return toWhereCondition(v.asDocument(), tableFilter, session);
                case "$eq": {
                    Expression left = toAggregateExpression(ba, 0, tableFilter, session);
                    Expression right = toAggregateExpression(ba, 1, tableFilter, session);
                    return new Comparison(session, Comparison.EQUAL, left, right);
                }
                case "$literal": {
                    if (v.isDocument()) {
                        v = new BsonString(v.asDocument().toJson());
                    }
                    return toValueExpression(v);
                }
                case "$getField": {
                    BsonDocument getFieldDoc = v.asDocument();
                    String field = getFieldDoc.getString("field").getValue().toUpperCase();
                    String input = getFieldDoc.getString("input").getValue();
                    if ("$$CURRENT".equals(input)) {
                        if ("_ID".equals(field)) {
                            field = Column.ROWID;
                        }
                        return getExpressionColumn(tableFilter, field);
                    }
                    throw DbException.getUnsupportedException("aggregate operator" + k);
                }
                default:
                    throw DbException.getUnsupportedException("aggregate operator " + k);
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

    public static Expression toAggregateExpression(BsonArray ba, int index, TableFilter tableFilter,
            ServerSession session) {
        BsonValue v = ba.get(index);
        if (v.isDocument()) {
            return toAggregateExpression(v.asDocument(), tableFilter, session);
        }
        return toValueExpression(v);
    }
}

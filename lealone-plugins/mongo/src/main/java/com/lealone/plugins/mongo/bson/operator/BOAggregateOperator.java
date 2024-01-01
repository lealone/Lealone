/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mongo.bson.operator;

import java.util.Map.Entry;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueInt;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.Operation;
import com.lealone.sql.expression.ValueExpression;
import com.lealone.sql.expression.condition.Comparison;
import com.lealone.sql.expression.condition.ConditionAndOr;
import com.lealone.sql.expression.condition.ConditionNot;
import com.lealone.sql.optimizer.TableFilter;

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
            if (k.charAt(0) == '$') {
                return toAggregateExpression(k, v, tableFilter, session);
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

    public static Expression toAggregateExpression(String k, BsonValue v, TableFilter tableFilter,
            ServerSession session) {
        Expression e = ArithmeticExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = ArrayExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = BitwiseOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = BooleanExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = ComparisonExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = ConditionalExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = CustomAggregationExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = DataSizeOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = DateExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = LiteralExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = MiscellaneousOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = ObjectExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = SetExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = StringExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = TextExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = TimestampExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = TrigonometryExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = TypeExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = AggregationAccumulatorOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = VariableExpressionOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;
        e = WindowOperator.toExpression(k, v, tableFilter, session);
        if (e != null)
            return e;

        switch (k) {
        case "$expr":
            return toWhereCondition(v.asDocument(), tableFilter, session);
        default:
            throw DbException.getUnsupportedException("aggregate operator " + k);
        }
    }

    // Lealone全倍支持MongoDB的ArithmeticExpressionOperator
    private static class ArithmeticExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$abs":
                return getFunction(session, "ABS", v);
            case "$add":
                return toOperation(v, Operation.PLUS);
            case "$ceil":
                return getFunction(session, "CEIL", v);
            case "$divide":
                return toOperation(v, Operation.DIVIDE);
            case "$exp":
                return getFunction(session, "EXP", v);
            case "$floor":
                return getFunction(session, "FLOOR", v);
            case "$ln":
                return getFunction(session, "LN", v);
            case "$log":
                return getFunction(session, "LOG", v);
            case "$log10":
                return getFunction(session, "LOG10", v);
            case "$mod":
                return getFunction(session, "MOD", v);
            case "$multiply":
                return toOperation(v, Operation.MULTIPLY);
            case "$pow":
                return getFunction(session, "POWER", v);
            case "$round":
                return getFunction(session, "ROUND", v);
            case "$sqrt":
                return getFunction(session, "SQRT", v);
            case "$subtract":
                return toOperation(v, Operation.MINUS);
            case "$trunc":
                return getFunction(session, "TRUNC", v);
            default:
                return null;
            }
        }

        private static Expression toOperation(BsonValue v, int opType) {
            BsonArray ba = v.asArray();
            Expression first = toValueExpression(ba.get(0));
            Operation o = null;
            for (int i = 1, size = ba.size(); i < size; i++) {
                Expression e = toValueExpression(ba.get(i));
                if (o == null)
                    o = new Operation(opType, first, e);
                else
                    o = new Operation(opType, o, e);
            }
            return o;
        }
    }

    // 未全倍支持
    private static class ArrayExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$arrayElemAt":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未全倍支持
    private static class BitwiseOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$bitAnd":
                return getFunction(session, "BITAND", v);
            case "$bitNot":
                throw getAUE(k);
            case "$bitOr":
                return getFunction(session, "BITOR", v);
            case "$bitXor":
                return getFunction(session, "BITXOR", v);
            default:
                return null;
            }
        }
    }

    // 全倍支持
    private static class BooleanExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            BsonArray ba = v.isArray() ? v.asArray() : null;
            switch (k) {
            case "$and":
                return toAndOrExpression(ba, tableFilter, session, ConditionAndOr.AND);
            case "$not":
                return new ConditionNot(toWhereCondition(v.asDocument(), tableFilter, session));
            case "$or":
                return toAndOrExpression(ba, tableFilter, session, ConditionAndOr.OR);
            default:
                return null;
            }
        }
    }

    // 全倍支持
    private static class ComparisonExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$cmp": {
                BsonArray ba = v.asArray();
                Value v1 = toValue(ba.get(0));
                Value v2 = toValue(ba.get(1));
                int ret = v1.compareTo(v2);
                return ValueExpression.get(ValueInt.get(ret));
            }
            case "$eq":
                return toComparison(v, tableFilter, session, Comparison.EQUAL);
            case "$gt":
                return toComparison(v, tableFilter, session, Comparison.BIGGER);
            case "$gte":
                return toComparison(v, tableFilter, session, Comparison.BIGGER_EQUAL);
            case "$lt":
                return toComparison(v, tableFilter, session, Comparison.SMALLER);
            case "$lte":
                return toComparison(v, tableFilter, session, Comparison.SMALLER_EQUAL);
            case "$ne":
                return toComparison(v, tableFilter, session, Comparison.NOT_EQUAL);
            default:
                return null;
            }
        }

        private static Expression toComparison(BsonValue v, TableFilter tableFilter,
                ServerSession session, int compareType) {
            BsonArray ba = v.asArray();
            Expression left = toAggregateExpression(ba, 0, tableFilter, session);
            Expression right = toAggregateExpression(ba, 1, tableFilter, session);
            return new Comparison(session, compareType, left, right);
        }
    }

    // 未支持
    private static class ConditionalExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$cond":
            case "$ifNull":
            case "$switch":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未支持
    private static class CustomAggregationExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$accumulator":
            case "$function":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未支持
    private static class DataSizeOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$binarySize":
            case "$bsonSize":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未全倍支持
    private static class DateExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$dateAdd":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 全倍支持
    private static class LiteralExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$literal": {
                if (v.isDocument()) {
                    v = new BsonString(v.asDocument().toJson());
                }
                return toValueExpression(v);
            }
            default:
                return null;
            }
        }
    }

    // 未全倍支持
    private static class MiscellaneousOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
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
                throw getAUE(k);
            }
            case "$rand":
                return getFunction(session, "RAND", v);
            case "$sampleRate":
                throw getAUE(k);
            case "$toHashedIndexKey":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未支持
    private static class ObjectExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$mergeObjects":
            case "$objectToArray":
            case "$setField":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未支持
    private static class SetExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$allElementsTrue":
            case "$anyElementTrue":
            case "$setDifference":
            case "$setEquals":
            case "$setIntersection":
            case "$setIsSubset":
            case "$setUnion":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未支持
    private static class StringExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$concat":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未支持
    private static class TextExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$meta":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未支持
    private static class TimestampExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$tsIncrement":
            case "$tsSecond":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未全倍支持
    private static class TrigonometryExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$sin":
                return getFunction(session, "SIN", v);
            case "$cos":
                return getFunction(session, "COS", v);
            case "$tan":
                return getFunction(session, "TAN", v);
            case "$asin":
                return getFunction(session, "ASIN", v);
            case "$acos":
                return getFunction(session, "ACOS", v);
            case "$atan":
                return getFunction(session, "ATAN", v);
            case "$atan2":
                return getFunction(session, "ATAN2", v);
            case "$asinh":
            case "$acoh":
            case "$atanh":
                throw getAUE(k);
            case "$sinh":
                return getFunction(session, "SINH", v);
            case "$cosh":
                return getFunction(session, "COSH", v);
            case "$tanh":
                return getFunction(session, "TANH", v);
            case "$degreesToRadians":
            case "$radiansToDegrees":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未支持
    private static class TypeExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$convert":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未支持
    private static class AggregationAccumulatorOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$accumulator":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未支持
    private static class VariableExpressionOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            switch (k) {
            case "$let":
                throw getAUE(k);
            default:
                return null;
            }
        }
    }

    // 未支持
    private static class WindowOperator extends BOAggregateOperator {

        public static Expression toExpression(String k, BsonValue v, TableFilter tableFilter,
                ServerSession session) {
            return null;
        }
    }
}

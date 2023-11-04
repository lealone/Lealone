/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.bson;

import java.sql.Date;
import java.util.ArrayList;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueBytes;
import org.lealone.db.value.ValueDate;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueString;
import org.lealone.plugins.mongo.bson.operator.BOQueryOperator;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.optimizer.TableFilter;

public abstract class BsonBase {

    public static DbException getUE(String message) {
        return DbException.getUnsupportedException(message);
    }

    public static Value toValue(BsonValue bv) {
        switch (bv.getBsonType()) {
        case INT32:
            return ValueInt.get(bv.asInt32().getValue());
        case INT64:
            return ValueLong.get(bv.asInt64().getValue());
        case OBJECT_ID:
            return ValueBytes.get(bv.asObjectId().getValue().toByteArray());
        case DATE_TIME:
            return ValueDate.get(new Date(bv.asDateTime().getValue()));
        case ARRAY:
            return toValueArray(bv.asArray());
        // case STRING:
        default:
            return ValueString.get(bv.asString().getValue());
        }
    }

    public static ValueArray toValueArray(BsonArray ba) {
        int size = ba.size();
        Value[] values = new Value[size];
        for (int i = 0; i < size; i++) {
            values[i] = toValue(ba.get(i));
        }
        return ValueArray.get(values);
    }

    public static BsonValue toBsonValue(String fieldName, Value v) {
        switch (v.getType()) {
        case Value.INT:
            return new BsonInt32(v.getInt());
        case Value.LONG:
            return new BsonInt64(v.getLong());
        case Value.NULL:
            return BsonNull.VALUE;
        case Value.ARRAY:
            return toBsonArray(fieldName, (ValueArray) v);
        case Value.BYTES:
            if (fieldName.equalsIgnoreCase("_id"))
                return new BsonObjectId(new ObjectId(v.getBytes()));
            else
                return new BsonBinary(v.getBytes());
        default:
            return new BsonString(v.getString());
        }
    }

    public static BsonArray toBsonArray(String fieldName, ValueArray va) {
        BsonArray ba = new BsonArray();
        Value[] values = va.getList();
        int length = values.length;
        for (int i = 0; i < length; i++) {
            ba.add(toBsonValue(fieldName, values[i]));
        }
        return ba;
    }

    public static BsonDocument toBsonDocument(String[] fieldNames, Value[] values) {
        int len = fieldNames.length;
        ArrayList<BsonElement> bsonElements = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            BsonValue bv = toBsonValue(fieldNames[i], values[i]);
            bsonElements.add(new BsonElement(fieldNames[i], bv));
        }
        return new BsonDocument(bsonElements);
    }

    public static ValueExpression toValueExpression(BsonValue bv) {
        return ValueExpression.get(toValue(bv));
    }

    public static Expression toWhereCondition(BsonDocument doc, TableFilter tableFilter,
            ServerSession session) {
        return BOQueryOperator.toWhereCondition(doc, tableFilter, session);
    }

    public static Column parseColumn(Table table, String columnName) {
        // if ("_id".equalsIgnoreCase(columnName)) {
        // return table.getRowIdColumn();
        // }
        return table.getColumn(columnName.toUpperCase());
    }

    public static ExpressionColumn getExpressionColumn(TableFilter tableFilter, String columnName) {
        return new ExpressionColumn(tableFilter.getTable().getDatabase(), tableFilter.getSchemaName(),
                tableFilter.getTableAlias(), columnName);
    }

    // 未使用
    public static Long getId(BsonDocument doc) {
        BsonValue id = doc.get("_id", null);
        if (id != null) {
            if (id.isInt32())
                return Long.valueOf(id.asInt32().getValue());
            else if (id.isInt64())
                return Long.valueOf(id.asInt64().getValue());
        }
        return null;
    }

    public static String getString(BsonDocument doc, String key) {
        return doc.getString(key).getValue();
    }

    public static String getStringOrNull(BsonDocument doc, String key) {
        BsonString v = doc.getString(key, null);
        return v == null ? null : v.getValue();
    }
}

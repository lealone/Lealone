/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.auth.User;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBytes;
import org.lealone.db.value.ValueDate;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueString;
import org.lealone.docdb.server.DocDBServer;
import org.lealone.docdb.server.DocDBServerConnection;
import org.lealone.docdb.server.DocDBTask;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.expression.condition.ConditionAndOr;
import org.lealone.sql.expression.condition.ConditionInConstantSet;
import org.lealone.sql.expression.condition.ConditionNot;
import org.lealone.sql.optimizer.TableFilter;

public abstract class BsonCommand {

    public static final Logger logger = LoggerFactory.getLogger(BsonCommand.class);
    public static final boolean DEBUG = false;

    public static BsonDocument newOkBsonDocument() {
        BsonDocument document = new BsonDocument();
        setOk(document);
        return document;
    }

    public static void setOk(BsonDocument doc) {
        append(doc, "ok", 1);
    }

    public static void setN(BsonDocument doc, int n) {
        append(doc, "n", n);
    }

    public static void setWireVersion(BsonDocument doc) {
        append(doc, "minWireVersion", 0);
        append(doc, "maxWireVersion", 17);
    }

    public static Database getDatabase(BsonDocument doc) {
        String dbName = doc.getString("$db").getValue();
        if (dbName == null)
            dbName = DocDBServer.DATABASE_NAME;
        Database db = LealoneDatabase.getInstance().findDatabase(dbName);
        if (db == null) {
            // 需要同步
            synchronized (LealoneDatabase.class) {
                db = LealoneDatabase.getInstance().findDatabase(dbName);
                if (db == null) {
                    String sql = "CREATE DATABASE IF NOT EXISTS " + dbName;
                    LealoneDatabase.getInstance().getSystemSession().prepareStatementLocal(sql)
                            .executeUpdate();
                    db = LealoneDatabase.getInstance().getDatabase(dbName);
                }
            }
        }
        if (!db.isInitialized())
            db.init();
        return db;
    }

    public static Table findTable(BsonDocument doc, String key, DocDBServerConnection conn) {
        Database db = getDatabase(doc);
        Schema schema = db.getSchema(null, Constants.SCHEMA_MAIN);
        String tableName = doc.getString(key).getValue();
        return schema.findTableOrView(null, tableName);
    }

    public static Column parseColumn(Table table, String columnName) {
        if ("_id".equalsIgnoreCase(columnName)) {
            return table.getRowIdColumn();
        }
        return table.getColumn(columnName.toUpperCase());
    }

    public static Table getTable(BsonDocument topDoc, BsonDocument firstDoc, String key,
            ServerSession session) {
        Database db = session.getDatabase();
        Schema schema = db.getSchema(null, Constants.SCHEMA_MAIN);
        String tableName = topDoc.getString(key).getValue();
        Table table = schema.findTableOrView(null, tableName);
        if (table == null) {
            StatementBuilder sql = new StatementBuilder();
            sql.append("CREATE TABLE IF NOT EXISTS ").append(Constants.SCHEMA_MAIN).append(".")
                    .append(tableName).append("(");
            for (Entry<String, BsonValue> e : firstDoc.entrySet()) {
                String columnName = e.getKey();
                if (columnName.equalsIgnoreCase("_id"))
                    continue;
                sql.appendExceptFirst(", ");
                sql.append(e.getKey()).append(" ");
                BsonValue v = e.getValue();
                switch (v.getBsonType()) {
                case INT32:
                    sql.append("int");
                    break;
                case INT64:
                    sql.append("long");
                    break;
                default:
                    sql.append("varchar");
                }
            }
            sql.append(")");
            session.prepareStatementLocal(sql.toString()).executeUpdate();
        }
        return schema.getTableOrView(null, tableName);
    }

    public static ServerSession createSession(Database db) {
        return new ServerSession(db, getUser(db), 0);
        // return db.createSession(getUser(db));
    }

    public static ServerSession getSession(Database db, DocDBServerConnection conn) {
        return conn.getPooledSession(db);
    }

    public static User getUser(Database db) {
        for (User user : db.getAllUsers()) {
            if (user.isAdmin())
                return user;
        }
        return db.getAllUsers().get(0);
    }

    public static void append(BsonDocument doc, String key, boolean value) {
        doc.append(key, new BsonBoolean(value));
    }

    public static void append(BsonDocument doc, String key, int value) {
        doc.append(key, new BsonInt32(value));
    }

    public static void append(BsonDocument doc, String key, long value) {
        doc.append(key, new BsonInt64(value));
    }

    public static void append(BsonDocument doc, String key, String value) {
        doc.append(key, new BsonString(value));
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
        // case STRING:
        default:
            return ValueString.get(bv.asString().getValue());
        }
    }

    public static ValueExpression toValueExpression(BsonValue bv) {
        return ValueExpression.get(toValue(bv));
    }

    public static BsonDocument toBsonDocument(String[] fieldNames, Value[] values) {
        int len = fieldNames.length;
        ArrayList<BsonElement> bsonElements = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            BsonValue bv = toBsonValue(values[i]);
            bsonElements.add(new BsonElement(fieldNames[i], bv));
        }
        return new BsonDocument(bsonElements);
    }

    public static BsonValue toBsonValue(Value v) {
        switch (v.getType()) {
        case Value.INT:
            return new BsonInt32(v.getInt());
        case Value.LONG:
            return new BsonInt64(v.getLong());
        case Value.NULL:
            return BsonNull.VALUE;
        default:
            return new BsonString(v.getString());
        }
    }

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

    public static ExpressionColumn getExpressionColumn(TableFilter tableFilter, String columnName) {
        return new ExpressionColumn(tableFilter.getTable().getDatabase(), tableFilter.getSchemaName(),
                tableFilter.getTableAlias(), columnName);
    }

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
        if (doc.size() > 1)
            throw DbException.getUnsupportedException(doc.toJson());
        String k = doc.getFirstKey();
        BsonValue v = doc.get(k);
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
            throw DbException.getUnsupportedException(doc.toJson());
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

    public static BsonDocument createResponseDocument(int n) {
        BsonDocument document = new BsonDocument();
        setOk(document);
        setN(document, n);
        return document;
    }

    public static void createAndSubmitYieldableUpdate(DocDBTask task, PreparedSQLStatement statement) {
        PreparedSQLStatement.Yieldable<?> yieldable = statement.createYieldableUpdate(ar -> {
            if (ar.isSucceeded()) {
                int updateCount = ar.getResult();
                BsonDocument response = createResponseDocument(updateCount);
                if (statement.getType() == SQLStatement.UPDATE)
                    append(response, "nModified", updateCount);
                task.conn.sendResponse(task.requestId, response);
            } else {
                task.conn.sendError(task.session, task.requestId, ar.getCause());
            }
        });
        task.si.submitYieldableCommand(task.requestId, yieldable);
    }

    public static ArrayList<BsonDocument> readPayload(ByteBufferBsonInput input, BsonDocument topDoc,
            DocDBServerConnection conn, Object key) {
        ArrayList<BsonDocument> documents = new ArrayList<>();
        BsonArray ba = topDoc.getArray(key, null);
        if (ba != null) {
            for (int i = 0, size = ba.size(); i < size; i++) {
                documents.add(ba.get(i).asDocument());
            }
        }

        // 对于insert、update、delete三个操作
        // mongodb-driver-sync会把documents包含在独立的payload中，需要特殊处理
        if (input.hasRemaining()) {
            input.readByte();
            input.readInt32(); // size
            input.readCString();
            while (input.hasRemaining()) {
                documents.add(conn.decode(input));
            }
        }
        return documents;
    }
}

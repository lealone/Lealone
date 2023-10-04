/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
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
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueMap;
import org.lealone.db.value.ValueString;
import org.lealone.docdb.server.DocDBServer;
import org.lealone.docdb.server.DocDBServerConnection;
import org.lealone.sql.expression.ValueExpression;

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
            String sql = "CREATE DATABASE IF NOT EXISTS " + dbName;
            LealoneDatabase.getInstance().getSystemSession().prepareStatementLocal(sql).executeUpdate();
            db = LealoneDatabase.getInstance().getDatabase(dbName);
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

    public static Table getTable(BsonDocument doc, String key, DocDBServerConnection conn) {
        Database db = getDatabase(doc);
        Schema schema = db.getSchema(null, Constants.SCHEMA_MAIN);
        String tableName = doc.getString(key).getValue();
        Table table = schema.findTableOrView(null, tableName);
        if (table == null) {
            try (ServerSession session = getSession(db, conn)) {
                String sql = "CREATE TABLE IF NOT EXISTS " + Constants.SCHEMA_MAIN + "." + tableName
                        + "(_json_ map<varchar, object>)";
                session.prepareStatementLocal(sql).executeUpdate();
            }
        }
        return schema.getTableOrView(null, tableName);
    }

    public static Column parseColumn(Table table, String columnName) {
        if ("_id".equalsIgnoreCase(columnName)) {
            return table.getRowIdColumn();
        }
        return table.getColumn(columnName.toUpperCase());
    }

    public static Table getTable(BsonDocument topDoc, BsonDocument firstDoc, String key,
            DocDBServerConnection conn) {
        Database db = getDatabase(topDoc);
        Schema schema = db.getSchema(null, Constants.SCHEMA_MAIN);
        String tableName = topDoc.getString(key).getValue();
        Table table = schema.findTableOrView(null, tableName);
        if (table == null) {
            try (ServerSession session = getSession(db, conn)) {
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
        }
        return schema.getTableOrView(null, tableName);
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

    public static ValueMap toValueMap(BsonDocument doc) {
        Value[] values = new Value[doc.size() * 2];
        int index = 0;
        for (Entry<String, BsonValue> e : doc.entrySet()) {
            values[index++] = ValueString.get(e.getKey());
            values[index++] = toValue(e.getValue());
        }
        return ValueMap.get(String.class, Object.class, values);
    }

    public static Value toValue(BsonValue bv) {
        switch (bv.getBsonType()) {
        case INT32:
            return ValueInt.get(bv.asInt32().getValue());
        case INT64:
            return ValueLong.get(bv.asInt64().getValue());
        case OBJECT_ID:
            return ValueBytes.get(bv.asObjectId().getValue().toByteArray());
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
        default:
            return new BsonString(v.getString());
        }
    }

    public static BsonDocument toBsonDocument(ValueMap vMap) {
        BsonDocument doc = (BsonDocument) vMap.getDocument();
        if (doc != null)
            return doc;
        synchronized (vMap) {
            doc = (BsonDocument) vMap.getDocument();
            if (doc != null)
                return doc;
            Map<Value, Value> map = vMap.getMap();
            ArrayList<BsonElement> bsonElements = new ArrayList<>(map.size());
            for (Entry<Value, Value> e : map.entrySet()) {
                BsonValue bv = toBsonValue(e.getValue());
                bsonElements.add(new BsonElement(e.getKey().getString(), bv));
            }
            doc = new BsonDocument(bsonElements);
            vMap.setDocument(doc);
            return doc;
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
}

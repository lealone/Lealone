/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mongo.server.bson.command;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.auth.User;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.mongo.server.MongoServer;
import org.lealone.mongo.server.MongoServerConnection;
import org.lealone.mongo.server.MongoTask;
import org.lealone.mongo.server.bson.BsonBase;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;

public abstract class BsonCommand extends BsonBase {

    public static final Logger logger = LoggerFactory.getLogger(BsonCommand.class);
    public static final boolean DEBUG = true;

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
            dbName = MongoServer.DATABASE_NAME;
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

    public static Table findTable(BsonDocument doc, String key, MongoServerConnection conn) {
        Database db = getDatabase(doc);
        Schema schema = db.getSchema(null, Constants.SCHEMA_MAIN);
        String tableName = doc.getString(key).getValue();
        return schema.findTableOrView(null, tableName);
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

    public static ServerSession getSession(Database db, MongoServerConnection conn) {
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

    public static BsonDocument createResponseDocument(int n) {
        BsonDocument document = new BsonDocument();
        setOk(document);
        setN(document, n);
        return document;
    }

    public static void createAndSubmitYieldableUpdate(MongoTask task, PreparedSQLStatement statement) {
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
            MongoServerConnection conn, Object key) {
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

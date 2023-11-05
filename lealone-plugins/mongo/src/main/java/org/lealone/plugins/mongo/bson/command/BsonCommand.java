/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.bson.command;

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
import org.lealone.db.table.Table;
import org.lealone.plugins.mongo.bson.BsonBase;
import org.lealone.plugins.mongo.bson.command.admin.AdminCommand;
import org.lealone.plugins.mongo.bson.command.auth.AuthCommand;
import org.lealone.plugins.mongo.bson.command.diagnostic.DiagnosticCommand;
import org.lealone.plugins.mongo.bson.command.index.IndexCommand;
import org.lealone.plugins.mongo.bson.command.role.RoleCommand;
import org.lealone.plugins.mongo.bson.command.sessions.SessionsCommand;
import org.lealone.plugins.mongo.bson.command.user.UserCommand;
import org.lealone.plugins.mongo.server.MongoServer;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;

public abstract class BsonCommand extends BsonBase {

    public static final Logger logger = LoggerFactory.getLogger(BsonCommand.class);
    public static final boolean DEBUG = false;
    public static final BsonDocument NOT_FOUND = new BsonDocument();

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
            createTable(tableName, firstDoc, session);
        }
        return schema.getTableOrView(null, tableName);
    }

    public static void createTable(String tableName, BsonDocument firstDoc, ServerSession session) {
        StatementBuilder sql = new StatementBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(Constants.SCHEMA_MAIN).append(".")
                .append(tableName).append("(_id ");
        BsonValue id = firstDoc.get("_id", null);
        if (id == null || id.isInt32() || id.isInt64()) {
            sql.append("long auto_increment primary key");
        } else {
            sql.append("binary primary key");
        }
        for (Entry<String, BsonValue> e : firstDoc.entrySet()) {
            String columnName = e.getKey();
            if (columnName.equalsIgnoreCase("_id"))
                continue;
            sql.append(", ");
            sql.append(columnName).append(" ");
            BsonValue v = e.getValue();
            switch (v.getBsonType()) {
            case INT32:
                sql.append("int");
                break;
            case INT64:
                sql.append("long");
                break;
            case DOUBLE:
                sql.append("double");
                break;
            case STRING:
                sql.append("varchar");
                break;
            case ARRAY:
                sql.append("array");
                break;
            case BINARY:
                sql.append("binary");
                break;
            case OBJECT_ID:
                sql.append("binary");
                break;
            case BOOLEAN:
                sql.append("boolean");
                break;
            // case DATE_TIME:
            // sql.append("datetime");
            // break;
            // case TIMESTAMP:
            // sql.append("timestamp");
            // break;
            case REGULAR_EXPRESSION:
                sql.append("varchar");
                break;
            case DECIMAL128:
                sql.append("decimal");
                break;
            case DOCUMENT:
                sql.append("map");
                // createTable(tableName + "_" + columnName, v.asDocument(), session);
                break;
            default:
                sql.append("varchar");
            }
        }
        sql.append(")");
        session.prepareStatementLocal(sql.toString()).executeUpdate();
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

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String command = doc.getFirstKey();
        switch (command) {
        // 最常用的几个crud命令
        case "insert":
            return BCInsert.execute(input, doc, conn, task);
        case "update":
            return BCUpdate.execute(input, doc, conn, task);
        case "delete":
            return BCDelete.execute(input, doc, conn, task);
        case "find":
            return BCFind.execute(input, doc, conn, task);
        case "aggregate":
            return BCAggregate.execute(input, doc, conn, task);
        // 握手命令
        case "hello":
        case "ismaster":
        case "isMaster": {
            BsonDocument document = new BsonDocument();
            BsonDocument speculativeAuthenticate = doc.getDocument("speculativeAuthenticate", null);
            if (speculativeAuthenticate != null) {
                BsonDocument res = AuthCommand.saslStart(input, speculativeAuthenticate, conn, task);
                BsonArray saslSupportedMechs = new BsonArray();
                saslSupportedMechs.add(new BsonString("SCRAM-SHA-256"));
                saslSupportedMechs.add(new BsonString("SCRAM-SHA-1"));
                document.append("saslSupportedMechs", saslSupportedMechs);
                document.append("speculativeAuthenticate", res);
            }
            append(document, "ismaster", true);
            append(document, "connectionId", conn.getConnectionId());
            append(document, "readOnly", false);
            setWireVersion(document);
            setOk(document);
            append(document, "isWritablePrimary", true);
            return document;
        }
        default:
            BsonDocument ret = AdminCommand.execute(input, doc, conn, command, task);
            if (ret == NOT_FOUND)
                ret = DiagnosticCommand.execute(input, doc, conn, command, task);
            if (ret == NOT_FOUND)
                ret = IndexCommand.execute(input, doc, conn, command, task);
            if (ret == NOT_FOUND)
                ret = UserCommand.execute(input, doc, conn, command, task);
            if (ret == NOT_FOUND)
                ret = RoleCommand.execute(input, doc, conn, command, task);
            if (ret == NOT_FOUND)
                ret = AuthCommand.execute(input, doc, conn, command, task);
            if (ret == NOT_FOUND)
                ret = SessionsCommand.execute(input, doc, conn, command, task);
            if (ret != NOT_FOUND)
                return ret;
            // BsonDocument document = new BsonDocument();
            // setWireVersion(document);
            // setOk(document);
            // setN(document, 0);
            throw getCUE(command);
        }
    }

    public static DbException getCUE(String command) {
        return DbException.getUnsupportedException("command " + command);
    }
}

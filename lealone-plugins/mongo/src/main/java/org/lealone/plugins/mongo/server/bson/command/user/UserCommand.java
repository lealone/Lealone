/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.command.user;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.auth.User;
import org.lealone.db.session.ServerSession;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;
import org.lealone.plugins.mongo.server.bson.command.BsonCommand;

public abstract class UserCommand extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, String command, MongoTask task) {
        switch (command) {
        case "createUser":
            return createUser(input, doc, conn, task);
        case "dropAllUsersFromDatabase":
            return dropAllUsersFromDatabase(input, doc, conn, task);
        case "dropUser":
            return dropUser(input, doc, conn, task);
        case "grantRolesToUser":
            return grantRolesToUser(input, doc, conn, task);
        case "revokeRolesFromUser":
            return revokeRolesFromUser(input, doc, conn, task);
        case "updateUser":
            return updateUser(input, doc, conn, task);
        case "usersInfo":
            return usersInfo(input, doc, conn, task);
        default:
            return NOT_FOUND;
        }
    }

    public static BsonDocument createUser(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String name = getString(doc, "createUser");
        String pwd = getString(doc, "pwd");
        Database db = getDatabase(doc);
        try (ServerSession session = getSession(db, conn)) {
            StatementBuilder sql = new StatementBuilder("CREATE USER IF NOT EXISTS ");
            sql.append('`').append(name).append('`').append(" PASSWORD '");
            sql.append(pwd).append("'");
            session.prepareStatementLocal(sql.toString()).executeUpdate();
        }
        return newOkBsonDocument();
    }

    public static BsonDocument dropAllUsersFromDatabase(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        Database db = getDatabase(doc);
        int n = 0;
        try (ServerSession session = getSession(db, conn)) {
            for (User u : db.getAllUsers()) {
                if (!u.isAdmin()) {
                    dropUser(session, u.getName());
                }
                n++;
            }
        }
        return createResponseDocument(n);
    }

    public static BsonDocument updateUser(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String name = getString(doc, "updateUser");
        String pwd = getString(doc, "pwd");
        Database db = getDatabase(doc);
        try (ServerSession session = getSession(db, conn)) {
            StatementBuilder sql = new StatementBuilder("ALTER USER ");
            sql.append('`').append(name).append('`').append(" SET PASSWORD '");
            sql.append(pwd).append("'");
            session.prepareStatementLocal(sql.toString()).executeUpdate();
        }
        return newOkBsonDocument();
    }

    public static BsonDocument dropUser(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String name = getString(doc, "dropUser");
        Database db = getDatabase(doc);
        try (ServerSession session = getSession(db, conn)) {
            dropUser(session, name);
        }
        return newOkBsonDocument();
    }

    private static void dropUser(ServerSession session, String name) {
        StatementBuilder sql = new StatementBuilder("DROP USER IF EXISTS ");
        sql.append('`').append(name).append('`');
        session.prepareStatementLocal(sql.toString()).executeUpdate();
    }

    public static BsonDocument usersInfo(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        BsonArray users = new BsonArray();
        BsonValue usersInfo = doc.get("usersInfo");
        if (usersInfo.isInt32()) {
            Database db = getDatabase(doc);
            for (User user : db.getAllUsers()) {
                addUserBsonDocument(users, user, db);
            }
        } else if (usersInfo.isDocument()) {
            addUserBsonDocument(users, usersInfo.asDocument(), doc);
        } else if (usersInfo.isArray()) {
            for (BsonValue user : usersInfo.asArray()) {
                addUserBsonDocument(users, user.asDocument(), doc);
            }
        }
        BsonDocument document = new BsonDocument();
        document.append("users", users);
        setOk(document);
        return document;
    }

    private static void addUserBsonDocument(BsonArray users, BsonDocument usersInfo, BsonDocument doc) {
        String userStr = getStringOrNull(usersInfo, "user");
        String dbStr = getStringOrNull(usersInfo, "db");
        Database db = dbStr == null ? getDatabase(doc)
                : LealoneDatabase.getInstance().getDatabase(dbStr);
        if (userStr != null) {
            User user = db.findUser(null, userStr);
            if (user != null)
                addUserBsonDocument(users, user, db);
        } else {
            for (User user : db.getAllUsers()) {
                addUserBsonDocument(users, user, db);
            }
        }
    }

    private static void addUserBsonDocument(BsonArray users, User user, Database db) {
        BsonDocument u = new BsonDocument();
        u.append("userId", new BsonInt32(user.getId()));
        u.append("user", new BsonString(user.getName()));
        u.append("db", new BsonString(db.getName()));
        users.add(u);
    }

    public static BsonDocument grantRolesToUser(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return newOkBsonDocument();
    }

    public static BsonDocument revokeRolesFromUser(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return newOkBsonDocument();
    }
}

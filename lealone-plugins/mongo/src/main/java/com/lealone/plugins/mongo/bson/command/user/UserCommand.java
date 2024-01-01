/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mongo.bson.command.user;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;

import com.lealone.common.util.StatementBuilder;
import com.lealone.db.Database;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.auth.User;
import com.lealone.plugins.mongo.bson.command.BsonCommand;
import com.lealone.plugins.mongo.server.MongoServerConnection;
import com.lealone.plugins.mongo.server.MongoTask;

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
        StatementBuilder sql = new StatementBuilder("CREATE USER IF NOT EXISTS ");
        sql.append('`').append(name).append('`').append(" PASSWORD '");
        sql.append(pwd).append("' ADMIN");
        conn.executeUpdateLocal(db, sql);
        grantRolesToUser(input, doc, conn, task, name);
        return newOkBsonDocument();
    }

    public static BsonDocument dropAllUsersFromDatabase(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        Database db = getDatabase(doc);
        int n = 0;
        for (User u : db.getAllUsers()) {
            if (!u.isAdmin()) {
                dropUser(conn, db, u.getName());
            }
            n++;
        }
        return createResponseDocument(n);
    }

    public static BsonDocument updateUser(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String name = getString(doc, "updateUser");
        String pwd = getString(doc, "pwd");
        Database db = getDatabase(doc);
        StatementBuilder sql = new StatementBuilder("ALTER USER ");
        sql.append('`').append(name).append('`').append(" SET PASSWORD '");
        sql.append(pwd).append("'");
        conn.executeUpdateLocal(db, sql);
        grantRolesToUser(input, doc, conn, task, name);
        return newOkBsonDocument();
    }

    public static BsonDocument dropUser(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String name = getString(doc, "dropUser");
        Database db = getDatabase(doc);
        dropUser(conn, db, name);
        return newOkBsonDocument();
    }

    private static void dropUser(MongoServerConnection conn, Database db, String name) {
        StatementBuilder sql = new StatementBuilder("DROP USER IF EXISTS ");
        sql.append('`').append(name).append('`');
        conn.executeUpdateLocal(db, sql);
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
        String user = getString(doc, "grantRolesToUser");
        grantRolesToUser(input, doc, conn, task, user);
        return newOkBsonDocument();
    }

    private static void grantRolesToUser(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task, String user) {
        BsonArray roles = doc.getArray("roles", null);
        Database db = getDatabase(doc);
        if (roles != null) {
            for (int i = 0, size = roles.size(); i < size; i++) {
                String roleName = null;
                if (roles.get(i).isString()) {
                    roleName = roles.get(i).asString().getValue();
                } else {
                    BsonDocument roleDoc = roles.get(i).asDocument();
                    String dbName = getStringOrNull(roleDoc, "db");
                    String roleName2 = getStringOrNull(roleDoc, "role");
                    if (db.getName().equalsIgnoreCase(dbName) && roleName2 != null
                            && !roleName2.isEmpty()) {
                        roleName = roleName2;
                    }
                }
                if (roleName != null) {
                    StatementBuilder sql = new StatementBuilder("GRANT");
                    sql.append(roleName).append(" TO ").append(user);
                    conn.executeUpdateLocal(db, sql);
                }
            }
        }
    }

    public static BsonDocument revokeRolesFromUser(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String user = getString(doc, "revokeRolesFromUser");
        BsonArray roles = doc.getArray("roles", null);
        Database db = getDatabase(doc);
        if (roles != null) {
            for (int i = 0, size = roles.size(); i < size; i++) {
                String roleName = null;
                if (roles.get(i).isString()) {
                    roleName = roles.get(i).asString().getValue();
                } else {
                    BsonDocument roleDoc = roles.get(i).asDocument();
                    String dbName = getStringOrNull(roleDoc, "db");
                    String roleName2 = getStringOrNull(roleDoc, "role");
                    if (db.getName().equalsIgnoreCase(dbName) && roleName2 != null
                            && !roleName2.isEmpty()) {
                        roleName = roleName2;
                    }
                }
                if (roleName != null) {
                    StatementBuilder sql = new StatementBuilder("REVOKE");
                    sql.append(roleName).append(" FROM ").append(user);
                    conn.executeUpdateLocal(db, sql);
                }
            }
        }
        return newOkBsonDocument();
    }
}

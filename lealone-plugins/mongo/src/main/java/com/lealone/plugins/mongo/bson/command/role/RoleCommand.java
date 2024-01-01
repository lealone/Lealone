/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mongo.bson.command.role;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;

import com.lealone.common.util.StatementBuilder;
import com.lealone.db.Database;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.auth.Role;
import com.lealone.plugins.mongo.bson.command.BsonCommand;
import com.lealone.plugins.mongo.server.MongoServerConnection;
import com.lealone.plugins.mongo.server.MongoTask;

public abstract class RoleCommand extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, String command, MongoTask task) {
        switch (command) {
        case "createRole":
            return createRole(input, doc, conn, task);
        case "dropRole":
            return dropRole(input, doc, conn, task);
        case "dropAllRolesFromDatabase":
            return dropAllRolesFromDatabase(input, doc, conn, task);
        case "grantPrivilegesToRole":
            return grantPrivilegesToRole(input, doc, conn, task);
        case "grantRolesToRole":
            return grantRolesToRole(input, doc, conn, task);
        case "invalidateUserCache":
            return invalidateUserCache(input, doc, conn, task);
        case "revokePrivilegesFromRole":
            return revokePrivilegesFromRole(input, doc, conn, task);
        case "revokeRolesFromRole":
            return revokeRolesFromRole(input, doc, conn, task);
        case "rolesInfo":
            return rolesInfo(input, doc, conn, task);
        case "updateRole":
            return updateRole(input, doc, conn, task);
        default:
            return NOT_FOUND;
        }
    }

    private static BsonDocument createRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String name = getString(doc, "createRole");
        Database db = getDatabase(doc);
        StatementBuilder sql = new StatementBuilder("CREATE ROLE IF NOT EXISTS ");
        sql.append('`').append(name).append('`');
        conn.executeUpdateLocal(db, sql);
        grantPrivilegesToRole(input, doc, conn, task, name);
        grantRolesToRole(input, doc, conn, task, name);
        return newOkBsonDocument();
    }

    private static BsonDocument dropRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String name = getString(doc, "dropRole");
        Database db = getDatabase(doc);
        StatementBuilder sql = new StatementBuilder("DROP ROLE IF EXISTS ");
        sql.append('`').append(name).append('`');
        conn.executeUpdateLocal(db, sql);
        return newOkBsonDocument();
    }

    private static BsonDocument dropAllRolesFromDatabase(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        Database db = getDatabase(doc);
        for (Role role : db.getAllRoles()) {
            StatementBuilder sql = new StatementBuilder("DROP ROLE IF EXISTS ");
            sql.append('`').append(role.getName()).append('`');
            conn.executeUpdateLocal(db, sql);
        }
        return newOkBsonDocument();
    }

    private static BsonDocument grantPrivilegesToRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String role = getString(doc, "grantPrivilegesToRole");
        grantPrivilegesToRole(input, doc, conn, task, role);
        return newOkBsonDocument();
    }

    private static void grantPrivilegesToRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task, String role) {
        BsonArray privileges = doc.getArray("privileges", null);
        Database db = getDatabase(doc);
        if (privileges != null) {
            for (int i = 0, size = privileges.size(); i < size; i++) {
                BsonDocument privilege = privileges.get(i).asDocument();
                BsonDocument resource = privilege.getDocument("resource");
                BsonArray actions = privilege.getArray("actions", null);
                if (actions != null) {
                    String dbName = getStringOrNull(resource, "db");
                    String collection = getStringOrNull(resource, "collection");
                    if (db.getName().equalsIgnoreCase(dbName) && collection != null
                            && !collection.isEmpty()) {
                        StatementBuilder sql = new StatementBuilder("GRANT");
                        for (int j = 0, actionSize = actions.size(); j < actionSize; j++) {
                            switch (actions.get(j).asString().getValue()) {
                            case "find":
                                sql.append(" SELECT");
                                break;
                            case "update":
                                sql.append(" UPDATE");
                                break;
                            case "insert":
                                sql.append(" INSERT");
                                break;
                            case "remove":
                                sql.append(" DELETE");
                                break;
                            }
                        }
                        sql.append(" ON ").append(collection).append(" TO ").append(role);
                        conn.executeUpdateLocal(db, sql);
                    }
                }
            }
        }
    }

    private static BsonDocument grantRolesToRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String role = getString(doc, "grantRolesToRole");
        grantRolesToRole(input, doc, conn, task, role);
        return newOkBsonDocument();
    }

    private static void grantRolesToRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task, String role) {
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
                    sql.append(roleName).append(" TO ").append(role);
                    conn.executeUpdateLocal(db, sql);
                }
            }
        }
    }

    private static BsonDocument invalidateUserCache(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return newOkBsonDocument();
    }

    private static BsonDocument revokePrivilegesFromRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String role = getString(doc, "revokePrivilegesFromRole");
        BsonArray privileges = doc.getArray("privileges", null);
        Database db = getDatabase(doc);
        if (privileges != null) {
            for (int i = 0, size = privileges.size(); i < size; i++) {
                BsonDocument privilege = privileges.get(i).asDocument();
                BsonDocument resource = privilege.getDocument("resource");
                BsonArray actions = privilege.getArray("actions", null);
                if (actions != null) {
                    String dbName = getStringOrNull(resource, "db");
                    String collection = getStringOrNull(resource, "collection");
                    if (db.getName().equalsIgnoreCase(dbName) && collection != null
                            && !collection.isEmpty()) {
                        StatementBuilder sql = new StatementBuilder("REVOKE");
                        for (int j = 0, actionSize = actions.size(); j < actionSize; j++) {
                            switch (actions.get(j).asString().getValue()) {
                            case "find":
                                sql.append(" SELECT");
                                break;
                            case "update":
                                sql.append(" UPDATE");
                                break;
                            case "insert":
                                sql.append(" INSERT");
                                break;
                            case "remove":
                                sql.append(" DELETE");
                                break;
                            }
                        }
                        sql.append(" ON ").append(collection).append(" FROM ").append(role);
                        conn.executeUpdateLocal(db, sql);
                    }
                }
            }
        }
        return newOkBsonDocument();
    }

    private static BsonDocument revokeRolesFromRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String role = getString(doc, "revokeRolesFromRole");
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
                    sql.append(roleName).append(" FROM ").append(role);
                    conn.executeUpdateLocal(db, sql);
                }
            }
        }
        return newOkBsonDocument();
    }

    private static BsonDocument rolesInfo(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        BsonArray roles = new BsonArray();
        BsonValue rolesInfo = doc.get("rolesInfo");
        if (rolesInfo.isInt32()) {
            Database db = getDatabase(doc);
            for (Role role : db.getAllRoles()) {
                addRoleBsonDocument(roles, role, db);
            }
        } else if (rolesInfo.isDocument()) {
            addRoleBsonDocument(roles, rolesInfo.asDocument(), doc);
        } else if (rolesInfo.isArray()) {
            for (BsonValue user : rolesInfo.asArray()) {
                addRoleBsonDocument(roles, user.asDocument(), doc);
            }
        } else if (rolesInfo.isString()) {
            Database db = getDatabase(doc);
            String roleName = rolesInfo.asString().getValue();
            Role role = db.findRole(null, roleName);
            addRoleBsonDocument(roles, role, db);
        }
        BsonDocument document = new BsonDocument();
        document.append("roles", roles);
        setOk(document);
        return document;
    }

    private static BsonDocument updateRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String name = getString(doc, "updateRole");
        grantPrivilegesToRole(input, doc, conn, task, name);
        grantRolesToRole(input, doc, conn, task, name);
        return newOkBsonDocument();
    }

    private static void addRoleBsonDocument(BsonArray roles, BsonDocument rolesInfo, BsonDocument doc) {
        String roleName = getStringOrNull(rolesInfo, "role");
        String dbName = getStringOrNull(rolesInfo, "db");
        Database db = dbName == null ? getDatabase(doc)
                : LealoneDatabase.getInstance().getDatabase(dbName);
        if (roleName != null) {
            Role role = db.findRole(null, roleName);
            if (role != null)
                addRoleBsonDocument(roles, role, db);
        } else {
            for (Role role : db.getAllRoles()) {
                addRoleBsonDocument(roles, role, db);
            }
        }
    }

    private static void addRoleBsonDocument(BsonArray roles, Role role, Database db) {
        BsonDocument u = new BsonDocument();
        u.append("_id", new BsonString(db.getName() + "." + role.getName()));
        u.append("roleId", new BsonInt32(role.getId()));
        u.append("role", new BsonString(role.getName()));
        u.append("db", new BsonString(db.getName()));
        u.append("roles", new BsonArray());
        u.append("inheritedRoles", new BsonArray());
        u.append("isBuiltin", new BsonBoolean(false));
        roles.add(u);
    }
}

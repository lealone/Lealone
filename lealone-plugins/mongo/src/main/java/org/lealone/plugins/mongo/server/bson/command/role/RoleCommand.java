/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.command.role;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;
import org.lealone.plugins.mongo.server.bson.command.BsonCommand;

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
        return null;
    }

    private static BsonDocument dropRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return null;
    }

    private static BsonDocument dropAllRolesFromDatabase(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return null;
    }

    private static BsonDocument grantPrivilegesToRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return null;
    }

    private static BsonDocument grantRolesToRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return null;
    }

    private static BsonDocument invalidateUserCache(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return null;
    }

    private static BsonDocument revokePrivilegesFromRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return null;
    }

    private static BsonDocument revokeRolesFromRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return null;
    }

    private static BsonDocument rolesInfo(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return null;
    }

    private static BsonDocument updateRole(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        return null;
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mongo.bson.command.index;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;

import com.lealone.plugins.mongo.bson.command.BsonCommand;
import com.lealone.plugins.mongo.server.MongoServerConnection;
import com.lealone.plugins.mongo.server.MongoTask;

public abstract class IndexCommand extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, String command, MongoTask task) {
        switch (command) {
        case "createIndexes":
            return ICCreateIndexes.execute(input, doc, conn, task);
        case "dropIndexes":
            return ICDropIndexes.execute(input, doc, conn, task);
        case "listIndexes":
            return ICListIndexes.execute(input, doc, conn, task);
        default:
            return NOT_FOUND;
        }
    }
}

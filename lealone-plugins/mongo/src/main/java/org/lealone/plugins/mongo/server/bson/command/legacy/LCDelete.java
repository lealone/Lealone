/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.command.legacy;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.plugins.mongo.server.MongoServerConnection;

public class LCDelete extends LegacyCommand {

    public static void execute(ByteBufferBsonInput input, MongoServerConnection conn) {
        input.readInt32(); // ZERO
        String fullCollectionName = input.readCString();
        input.readInt32(); // flags
        BsonDocument selector = conn.decode(input);
        if (DEBUG)
            logger.info("delete: {} {}", fullCollectionName, selector.toJson());
        input.close();
        // 不需要返回响应
    }
}

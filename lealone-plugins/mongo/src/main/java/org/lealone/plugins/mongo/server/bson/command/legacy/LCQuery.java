/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.command.legacy;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.bson.command.BCOther;

public class LCQuery extends LegacyCommand {

    public static void execute(ByteBufferBsonInput input, int requestId, MongoServerConnection conn) {
        input.readInt32(); // flags
        String fullCollectionName = input.readCString();
        input.readInt32(); // numberToSkip
        input.readInt32(); // numberToReturn
        BsonDocument doc = conn.decode(input);
        if (DEBUG)
            logger.info("query: {} {}", fullCollectionName, doc.toJson());
        if (input.hasRemaining()) {
            BsonDocument returnFieldsSelector = conn.decode(input);
            if (DEBUG)
                logger.info("returnFieldsSelector: {}", returnFieldsSelector.toJson());
        }
        input.close();
        String command = doc.getFirstKey().toLowerCase();
        if (command.equals("ismaster")) {
            conn.sendResponse(requestId, BCOther.execute(input, doc, conn, command, null));
        } else {
            conn.sendResponse(requestId);
        }
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.bson.command.legacy;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.docdb.server.DocDBServerConnection;

public class LCQuery extends LegacyCommand {

    public static void execute(ByteBufferBsonInput input, int requestId, DocDBServerConnection conn) {
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
        conn.sendResponse(requestId);
    }
}

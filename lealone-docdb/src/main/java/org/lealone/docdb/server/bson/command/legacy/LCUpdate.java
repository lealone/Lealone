/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.bson.command.legacy;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.docdb.server.DocDBServerConnection;

public class LCUpdate extends LegacyCommand {

    public static void execute(ByteBufferBsonInput input, DocDBServerConnection conn) {
        input.readInt32(); // ZERO
        String fullCollectionName = input.readCString();
        input.readInt32(); // flags
        BsonDocument selector = conn.decode(input);
        BsonDocument update = conn.decode(input);
        if (DEBUG)
            logger.info("update: {} {} {}", fullCollectionName, update.toJson(), selector.toJson());
        input.close();
        // 不需要返回响应
    }
}

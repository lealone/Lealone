/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.legacy;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.docdb.server.DocDBServerConnection;
import org.lealone.docdb.server.command.BsonCommand;

public class OpcodeInsert extends BsonCommand {

    public static void execute(ByteBufferBsonInput input, DocDBServerConnection conn) {
        input.readInt32(); // flags
        String fullCollectionName = input.readCString();
        while (input.hasRemaining()) {
            BsonDocument doc = conn.decode(input);
            if (DEBUG)
                logger.info("insert: {} {}", fullCollectionName, doc.toJson());
        }
        input.close();
        // 不需要返回响应
    }
}

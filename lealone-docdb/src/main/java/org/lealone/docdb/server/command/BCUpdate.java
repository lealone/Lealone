/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.docdb.server.DocDBServerConnection;

public class BCUpdate extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            DocDBServerConnection conn) {
        int n = 0;
        BsonArray updates = doc.getArray("updates", null);
        if (updates != null) {
            for (int i = 0, size = updates.size(); i < size; i++) {
                BsonDocument update = updates.get(i).asDocument();
                if (DEBUG)
                    logger.info(update.toJson());
                BsonDocument q = update.getDocument("q", null);
                BsonDocument u = update.getDocument("u", null);
                if (u == null)
                    continue;
                if (q != null) {
                }
            }
        }
        BsonDocument document = new BsonDocument();
        setOk(document);
        setN(document, n);
        return document;
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mongo.server;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.session.ServerSession;
import org.lealone.mongo.server.bson.command.BCAggregate;
import org.lealone.mongo.server.bson.command.BCDelete;
import org.lealone.mongo.server.bson.command.BCFind;
import org.lealone.mongo.server.bson.command.BCInsert;
import org.lealone.mongo.server.bson.command.BCOther;
import org.lealone.mongo.server.bson.command.BCUpdate;
import org.lealone.mongo.server.bson.command.index.ICCreateIndexes;
import org.lealone.mongo.server.bson.command.index.ICDropIndexes;
import org.lealone.mongo.server.bson.command.index.ICListIndexes;
import org.lealone.net.NetBuffer;
import org.lealone.server.LinkableTask;
import org.lealone.server.SessionInfo;

public class MongoTask extends LinkableTask {

    private static final Logger logger = LoggerFactory.getLogger(MongoTask.class);

    public final MongoServerConnection conn;
    public final ByteBufferBsonInput input;
    public final BsonDocument doc;
    public final ServerSession session;
    public final SessionInfo si;
    public final int requestId;
    public final NetBuffer buffer;

    public MongoTask(MongoServerConnection conn, ByteBufferBsonInput input, BsonDocument doc,
            SessionInfo si, int requestId, NetBuffer buffer) {
        this.conn = conn;
        this.input = input;
        this.doc = doc;
        this.session = si.getSession();
        this.si = si;
        this.requestId = requestId;
        this.buffer = buffer;
    }

    @Override
    public void run() {
        try {
            BsonDocument response = executeCommand();
            if (response != null) {
                conn.sendResponse(requestId, response);
            }
        } catch (Throwable e) {
            logger.error("Failed to execute command: {}", e, doc.getFirstKey());
            conn.sendError(session, requestId, e);
        } finally {
            // 确保无论出现什么情况都回收
            buffer.recycle();
            input.close();
        }
    }

    private BsonDocument executeCommand() throws Exception {
        String command = doc.getFirstKey().toLowerCase();
        switch (command) {
        case "insert":
            return BCInsert.execute(input, doc, conn, this);
        case "update":
            return BCUpdate.execute(input, doc, conn, this);
        case "delete":
            return BCDelete.execute(input, doc, conn, this);
        case "find":
            return BCFind.execute(input, doc, conn, this);
        case "aggregate":
            return BCAggregate.execute(input, doc, conn, this);
        case "createindexes":
            return ICCreateIndexes.execute(input, doc, conn, this);
        case "dropindexes":
            return ICDropIndexes.execute(input, doc, conn, this);
        case "listindexes":
            return ICListIndexes.execute(input, doc, conn, this);
        default:
            return BCOther.execute(input, doc, conn, command);
        }
    }
}

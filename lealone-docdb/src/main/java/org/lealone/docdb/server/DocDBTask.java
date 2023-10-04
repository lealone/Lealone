/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.session.ServerSession;
import org.lealone.docdb.server.command.BCAggregate;
import org.lealone.docdb.server.command.BCDelete;
import org.lealone.docdb.server.command.BCFind;
import org.lealone.docdb.server.command.BCInsert;
import org.lealone.docdb.server.command.BCOther;
import org.lealone.docdb.server.command.BCUpdate;
import org.lealone.net.NetBuffer;
import org.lealone.server.LinkableTask;
import org.lealone.server.SessionInfo;

public class DocDBTask extends LinkableTask {

    private static final Logger logger = LoggerFactory.getLogger(DocDBTask.class);

    public final DocDBServerConnection conn;
    public final ByteBufferBsonInput input;
    public final BsonDocument doc;
    public final ServerSession session;
    public final SessionInfo si;
    public final int requestId;
    public final NetBuffer buffer;

    public DocDBTask(DocDBServerConnection conn, ByteBufferBsonInput input, BsonDocument doc,
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
            conn.sendError(session, 0, e);
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
        default:
            return BCOther.execute(input, doc, conn, command);
        }
    }
}

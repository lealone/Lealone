/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mongo.bson.command.diagnostic;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.io.ByteBufferBsonInput;

import com.lealone.plugins.mongo.bson.command.BsonCommand;
import com.lealone.plugins.mongo.server.MongoServerConnection;
import com.lealone.plugins.mongo.server.MongoTask;

public abstract class DiagnosticCommand extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, String command, MongoTask task) {
        switch (command) {
        case "buildInfo": {
            BsonDocument document = new BsonDocument();
            append(document, "version", "6.0.0");
            setOk(document);
            return document;
        }
        case "collStats":
        case "connPoolStats":
        case "connectionStatus":
        case "getCmdLineOpts":
        case "getLog":
        case "ping":
        case "listCommands":
        case "top":
        case "_isSelf":
        case "hostInfo":
            return newOkBsonDocument();
        case "lockInfo":
            return newOkBsonDocument().append("lockInfo", new BsonArray());
        case "whatsmyuri":
            return newOkBsonDocument().append("you", new BsonString(
                    conn.getWritableChannel().getHost() + ":" + conn.getWritableChannel().getPort()));
        default:
            return NOT_FOUND;
        }
    }
}

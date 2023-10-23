/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.command.diagnostic;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;
import org.lealone.plugins.mongo.server.bson.command.BsonCommand;

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
        case "getCmdLineOpts":
        case "getLog":
        case "ping":
            return newOkBsonDocument();
        default:
            return NOT_FOUND;
        }
    }
}

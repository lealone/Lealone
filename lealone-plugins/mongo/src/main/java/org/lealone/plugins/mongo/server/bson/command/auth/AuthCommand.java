/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.command.auth;

import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.auth.User;
import org.lealone.db.auth.scram.ScramPasswordData;
import org.lealone.db.auth.scram.ScramPasswordHash;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;
import org.lealone.plugins.mongo.server.bson.command.BsonCommand;

public abstract class AuthCommand extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, String command, MongoTask task) {
        switch (command) {
        case "saslStart":
        case "authenticate":
            return saslStart(input, doc, conn, task);
        case "saslContinue":
            return saslContinue(input, doc, conn, task);
        case "logout":
            return newOkBsonDocument();
        default:
            return NOT_FOUND;
        }
    }

    public static BsonDocument saslStart(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String name = getString(doc, "db");
        Database db = LealoneDatabase.getInstance().getDatabase(name);
        ScramSaslSender sender = new ScramSaslSender();
        ScramSaslProcessor processor;
        String mechanism = getString(doc, "mechanism");
        if (mechanism.equalsIgnoreCase("SCRAM-SHA-256")) {
            ScramSaslUserDataLoader loader = new ScramSaslUserDataLoader(256, db);
            processor = ScramSaslProcessor.create(conn.getConnectionId(), loader, sender, 256);
        } else {
            ScramSaslUserDataLoader loader = new ScramSaslUserDataLoader(1, db);
            processor = ScramSaslProcessor.create(conn.getConnectionId(), loader, sender, 1);
        }
        conn.setScramSaslServerProcessor(processor);
        BsonBinary payload = doc.getBinary("payload");
        String str = new String(payload.getData());
        processor.onMessage(str);
        return sender.document;
    }

    public static BsonDocument saslContinue(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        ScramSaslProcessor server = conn.getScramSaslServerProcessor();
        BsonBinary payload = doc.getBinary("payload");
        String str = new String(payload.getData());
        server.onMessage(str);
        return ((ScramSaslSender) server.getSender()).document;
    }

    private static class ScramSaslSender implements ScramSaslProcessor.Sender {

        private BsonDocument document;

        @Override
        public void sendMessage(long connectionId, String msg, ScramSaslProcessor processor) {
            document = new BsonDocument();
            append(document, "conversationId", (int) connectionId); // 只能用int类型，否则客户端报错
            append(document, "done", processor.isEnded());

            BsonBinary payload = new BsonBinary(msg.getBytes());
            document.append("payload", payload);
            setOk(document);
        }
    }

    private static class ScramSaslUserDataLoader implements ScramSaslProcessor.UserDataLoader {

        private final int mechanism;
        private final Database db;

        ScramSaslUserDataLoader(int mechanism, Database db) {
            this.mechanism = mechanism;
            this.db = db;
        }

        @Override
        public void loadUserData(String userName, long connectionId, ScramSaslProcessor processor) {
            User user = db.getUser(null, userName);
            ScramPasswordData data = ScramPasswordHash.createScramPasswordData(user.getSaltMongo(),
                    user.getPasswordHashMongo(), mechanism);
            processor.onUserDataLoaded(data);
        }
    }
}

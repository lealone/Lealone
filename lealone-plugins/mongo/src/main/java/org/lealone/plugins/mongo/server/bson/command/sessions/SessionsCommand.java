/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.command.sessions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;
import org.lealone.plugins.mongo.server.bson.command.BsonCommand;

public class SessionsCommand extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, String command, MongoTask task) {
        switch (command) {
        case "abortTransaction":
        case "commitTransaction": {
            throw getCUE(command);
        }
        case "endSessions": {
            for (UUID id : decodeUUIDs(doc, "endSessions")) {
                ServerSession session = conn.getSessions().remove(id);
                if (session != null)
                    session.close();
            }
            return newOkBsonDocument();
        }
        case "killAllSessions":
        case "killAllSessionsByPattern":
        case "killSessions": {
            for (UUID id : decodeUUIDs(doc, "killSessions")) {
                ServerSession session = conn.getSessions().remove(id);
                if (session != null)
                    session.close();
            }
            return newOkBsonDocument();
        }
        case "refreshSessions": {
            for (UUID id : decodeUUIDs(doc, "refreshSessions")) {
                ServerSession session = conn.getSessions().get(id);
                if (session != null)
                    session.close();
            }
            return newOkBsonDocument();
        }
        case "startSession": {
            Database db = getDatabase(doc);
            ServerSession session = createSession(db);
            UUID id = UUID.randomUUID();
            conn.getSessions().put(id, session);
            BsonDocument document = new BsonDocument();
            document.append("id", new BsonBinary(id));
            append(document, "timeoutMinutes", 30);
            setOk(document);
            return document;
        }
        default:
            return NOT_FOUND;
        }
    }

    private static List<UUID> decodeUUIDs(BsonDocument doc, Object key) {
        BsonArray ba = doc.getArray(key, null);
        if (ba != null) {
            int size = ba.size();
            List<UUID> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                BsonDocument s = ba.get(i).asDocument();
                UUID id = s.getBinary("id").asUuid();
                list.add(id);
            }
            return list;
        }
        return Collections.emptyList();
    }
}

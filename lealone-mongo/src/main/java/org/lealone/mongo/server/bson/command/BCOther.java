/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mongo.server.bson.command;

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
import org.lealone.mongo.server.MongoServerConnection;

public class BCOther extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, String command) {
        switch (command) {
        case "hello":
        case "ismaster": {
            BsonDocument document = new BsonDocument();
            append(document, "ismaster", true);
            append(document, "connectionId", conn.getConnectionId());
            append(document, "readOnly", false);
            setWireVersion(document);
            setOk(document);
            append(document, "isWritablePrimary", true);
            return document;
        }
        case "buildinfo": {
            BsonDocument document = new BsonDocument();
            append(document, "version", "6.0.0");
            setOk(document);
            return document;
        }
        case "getparameter": {
            BsonDocument document = new BsonDocument();
            BsonDocument v = new BsonDocument();
            append(v, "version", "6.0");
            document.append("featureCompatibilityVersion", v);
            setOk(document);
            return document;
        }
        case "startsession": {
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
        case "killsessions": {
            for (UUID id : decodeUUIDs(doc, "killSessions")) {
                ServerSession session = conn.getSessions().remove(id);
                if (session != null)
                    session.close();
            }
            return newOkBsonDocument();
        }
        case "refreshsessions": {
            for (UUID id : decodeUUIDs(doc, "refreshSessions")) {
                ServerSession session = conn.getSessions().get(id);
                if (session != null)
                    session.close();
            }
            return newOkBsonDocument();
        }
        case "endsessions": {
            for (UUID id : decodeUUIDs(doc, "endSessions")) {
                ServerSession session = conn.getSessions().remove(id);
                if (session != null)
                    session.close();
            }
            return newOkBsonDocument();
        }
        default:
            BsonDocument document = new BsonDocument();
            setWireVersion(document);
            setOk(document);
            setN(document, 0);
            return document;
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

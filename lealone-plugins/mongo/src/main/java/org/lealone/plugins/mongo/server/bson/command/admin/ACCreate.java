/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.command.admin;

import java.util.Map.Entry;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.session.ServerSession;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;

public class ACCreate extends AdminCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        String name = doc.getString("create").getValue();
        BsonString viewOn = doc.getString("viewOn", null);
        if (viewOn == null) {
            try (ServerSession session = getSession(getDatabase(doc), conn)) {
                String sql = "CREATE TABLE IF NOT EXISTS " + name;
                session.prepareStatementLocal(sql).executeUpdate();
            }
        } else {
            BsonArray pipeline = doc.getArray("pipeline");
            BsonDocument project = pipeline.get(0).asDocument().getDocument("$project");
            StatementBuilder sql = new StatementBuilder("CREATE VIEW IF NOT EXISTS ");
            sql.append(name).append(" AS SELECT ");
            for (Entry<String, BsonValue> e : project.entrySet()) {
                sql.appendExceptFirst(", ");
                sql.append(e.getKey());
            }
            sql.append(" FROM ").append(viewOn.getValue());
            try (ServerSession session = getSession(getDatabase(doc), conn)) {
                session.prepareStatementLocal(sql.toString()).executeUpdate();
            }
        }
        BsonDocument document = new BsonDocument();
        setOk(document);
        return document;
    }
}

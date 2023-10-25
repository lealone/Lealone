/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.bson.command.index;

import java.util.Map.Entry;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;

public class ICCreateIndexes extends IndexCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        Table table = findTable(doc, "createIndexes", conn);
        if (table != null) {
            try (ServerSession session = getSession(table.getDatabase(), conn)) {
                BsonArray indexes = doc.getArray("indexes");
                for (int i = 0, size = indexes.size(); i < size; i++) {
                    BsonDocument index = indexes.get(i).asDocument();
                    String name = index.getString("name").getValue();
                    BsonDocument key = index.getDocument("key");
                    StatementBuilder sql = new StatementBuilder("CREATE INDEX IF NOT EXISTS ");
                    sql.append('`').append(name).append('`').append(" ON ");
                    sql.append(table.getSQL()).append('(');
                    for (Entry<String, BsonValue> e : key.entrySet()) {
                        sql.appendExceptFirst(", ");
                        sql.append(e.getKey());
                        BsonValue v = e.getValue();
                        if (v.isNumber() && v.asNumber().longValue() < 0)
                            sql.append(" DESC");
                    }
                    sql.append(')');
                    session.prepareStatementLocal(sql.toString()).executeUpdate();
                }
            }
        }
        return createResponseDocument(0);
    }
}

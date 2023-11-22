/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.bson.command.index;

import java.util.ArrayList;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.index.Index;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;

public class ICDropIndexes extends IndexCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        Table table = findTable(doc, "dropIndexes", conn);
        if (table != null) {
            try (ServerSession session = getSession(table.getDatabase(), conn)) {
                String name = doc.getString("index").getValue();
                if (name.equals("*")) {
                    // 需要copy一份，否则执行drop index时会删除table.getIndexes()对应的List的元素
                    // 边遍历边删除List的元素会出现ConcurrentModificationException
                    ArrayList<Index> indexes = new ArrayList<>(table.getIndexes());
                    for (Index index : indexes) {
                        if (index.isRowIdIndex())
                            continue;
                        dropIndex(session, index.getName());
                    }
                } else {
                    dropIndex(session, name);
                }
            }
        }
        return createResponseDocument(0);
    }

    private static void dropIndex(ServerSession session, String name) {
        StatementBuilder sql = new StatementBuilder("DROP INDEX IF EXISTS ");
        sql.append('`').append(name).append('`');
        session.executeUpdateLocal(sql.toString());
    }
}

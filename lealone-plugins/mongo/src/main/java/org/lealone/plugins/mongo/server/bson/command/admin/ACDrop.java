/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.command.admin;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;

public class ACDrop extends AdminCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        Table table = findTable(doc, "drop", conn); // 可能是一个表或视图
        if (table != null && table.getDropSQL() != null) {
            try (ServerSession session = getSession(table.getDatabase(), conn)) {
                String sql = table.getDropSQL();
                session.prepareStatementLocal(sql).executeUpdate();
            }
        }
        BsonDocument document = new BsonDocument();
        setOk(document);
        return document;
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mongo.bson.command.admin;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;

import com.lealone.db.table.Table;
import com.lealone.plugins.mongo.server.MongoServerConnection;
import com.lealone.plugins.mongo.server.MongoTask;

public class ACDrop extends AdminCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        Table table = findTable(doc, "drop", conn); // 可能是一个表或视图
        if (table != null && table.getDropSQL() != null) {
            String sql = table.getDropSQL();
            conn.executeUpdateLocal(table.getDatabase(), sql);
        }
        BsonDocument document = new BsonDocument();
        setOk(document);
        return document;
    }
}

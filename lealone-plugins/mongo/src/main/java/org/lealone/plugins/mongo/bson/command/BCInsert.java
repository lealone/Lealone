/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.bson.command;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.Utils;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;
import org.lealone.sql.dml.Insert;
import org.lealone.sql.expression.Expression;

public class BCInsert extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument topDoc,
            MongoServerConnection conn, MongoTask task) {
        ArrayList<BsonDocument> documents = readPayload(input, topDoc, conn, "documents");
        int size = documents.size();
        if (size > 0) {
            addRows(topDoc, conn, documents, size, task);
            return null;
        } else {
            return createResponseDocument(0);
        }
    }

    private static void addRows(BsonDocument topDoc, MongoServerConnection conn,
            ArrayList<BsonDocument> documents, int size, MongoTask task) {
        ServerSession session = task.session;
        Table table = getTable(topDoc, documents.get(0), "insert", session);
        Insert insert = new Insert(session);
        insert.setTable(table);
        for (int i = 0; i < size; i++) {
            ArrayList<Column> columns = Utils.newSmallArrayList();
            ArrayList<Expression> values = Utils.newSmallArrayList();
            HashSet<Column> set = new HashSet<>();
            BsonDocument document = documents.get(i);
            for (Entry<String, BsonValue> e : document.entrySet()) {
                Column column = parseColumn(table, e.getKey());
                if (!set.add(column)) {
                    throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getSQL());
                }
                columns.add(column);
                values.add(toValueExpression(e.getValue()));
            }
            insert.setColumns(columns.toArray(new Column[columns.size()]));
            insert.addRow(values.toArray(new Expression[values.size()]));
        }
        insert.prepare();
        createAndSubmitYieldableUpdate(task, insert);
    }
}

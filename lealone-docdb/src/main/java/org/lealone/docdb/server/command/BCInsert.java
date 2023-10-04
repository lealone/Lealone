/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.Utils;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.docdb.server.DocDBServerConnection;
import org.lealone.docdb.server.DocDBTask;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.dml.Insert;
import org.lealone.sql.expression.Expression;

public class BCInsert extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument topDoc,
            DocDBServerConnection conn, DocDBTask task) {
        ArrayList<BsonDocument> list = new ArrayList<>();
        BsonArray documents = topDoc.getArray("documents", null);
        if (documents != null) {
            for (int i = 0, size = documents.size(); i < size; i++) {
                list.add(documents.get(i).asDocument());
            }
        }
        // mongodb-driver-sync会把documents包含在独立的payload中，需要特殊处理
        if (input.hasRemaining()) {
            input.readByte();
            input.readInt32(); // size
            input.readCString();
            while (input.hasRemaining()) {
                list.add(conn.decode(input));
            }
        }
        int size = list.size();
        if (size > 0) {
            addRows(topDoc, conn, list, size, task);
        }
        return null;
    }

    private static void addRows(BsonDocument topDoc, DocDBServerConnection conn,
            ArrayList<BsonDocument> documents, int size, DocDBTask task) {
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

        PreparedSQLStatement.Yieldable<?> yieldable = insert.createYieldableUpdate(ar -> {
            if (ar.isSucceeded()) {
                int updateCount = ar.getResult();
                BsonDocument document = new BsonDocument();
                setOk(document);
                setN(document, updateCount);
                conn.sendResponse(task.requestId, document);
            } else {
                conn.sendError(session, -1, ar.getCause());
            }
        });
        task.si.submitYieldableCommand(-1, yieldable);
    }

    @SuppressWarnings("unused")
    private static void addRowsOld(BsonDocument topDoc, DocDBServerConnection conn,
            ArrayList<BsonDocument> list, int size) {
        Table table = getTable(topDoc, "insert", conn);
        ServerSession session = getSession(table.getDatabase(), conn);
        AtomicInteger counter = new AtomicInteger(size);
        AtomicBoolean isFailed = new AtomicBoolean(false);
        for (int i = 0; i < size && !isFailed.get(); i++) {
            BsonDocument document = list.get(i);
            Row row = table.getTemplateRow();
            row.setValue(0, toValueMap(document));
            Long id = getId(document);
            if (id != null) {
                row.setKey(id.longValue());
            }
            table.addRow(session, row).onComplete(ar -> {
                if (isFailed.get())
                    return;
                if (ar.isFailed()) {
                    isFailed.set(true);
                    session.rollback();
                }
                if (counter.decrementAndGet() == 0 || isFailed.get()) {
                    session.asyncCommit(() -> session.close());
                }
            });
        }
    }
}

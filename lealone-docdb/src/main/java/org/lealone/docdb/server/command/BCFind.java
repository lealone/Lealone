/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import java.util.ArrayList;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.util.Utils;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.docdb.server.DocDBServerConnection;
import org.lealone.docdb.server.DocDBTask;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.Wildcard;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.query.Select;

public class BCFind extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            DocDBServerConnection conn, DocDBTask task) {
        Table table = findTable(doc, "find", conn);
        if (table != null) {
            find(doc, conn, table, task);
            return null;
        } else {
            BsonArray documents = new BsonArray();
            return createResponseDocument(doc, documents);
        }
    }

    private static BsonDocument createResponseDocument(BsonDocument doc, BsonArray documents) {
        BsonDocument document = new BsonDocument();
        BsonDocument cursor = new BsonDocument();
        append(cursor, "id", 0L);
        append(cursor, "ns", doc.getString("$db").getValue() + "." + doc.getString("find").getValue());
        cursor.append("firstBatch", documents);
        document.append("cursor", cursor);
        setOk(document);
        return document;
    }

    private static void find(BsonDocument doc, DocDBServerConnection conn, Table table, DocDBTask task) {
        ServerSession session = task.session;
        Select select = new Select(session);
        TableFilter tableFilter = new TableFilter(session, table, null, true, select);
        select.addTableFilter(tableFilter, true);
        BsonDocument filter = doc.getDocument("filter", null);
        if (filter != null) {
            if (DEBUG)
                logger.info("filter: {}", filter.toJson());
            select.addCondition(toWhereCondition(filter, tableFilter, session));
        }

        ArrayList<Expression> selectExpressions = Utils.newSmallArrayList();
        selectExpressions.add(getExpressionColumn(tableFilter, Column.ROWID)); // 总是获取rowid
        selectExpressions.add(new Wildcard(tableFilter.getSchemaName(), tableFilter.getTableAlias()));
        select.setExpressions(selectExpressions);
        select.init();
        select.prepare();

        PreparedSQLStatement.Yieldable<?> yieldable = select.createYieldableQuery(-1, false, ar -> {
            if (ar.isSucceeded()) {
                Result result = ar.getResult();
                task.conn.sendResponse(task.requestId,
                        createResponseDocument(task.doc, toBsonDocuments(result)));
            } else {
                task.conn.sendError(task.session, -1, ar.getCause());
            }
        });
        task.si.submitYieldableCommand(-1, yieldable);
    }

    private static BsonArray toBsonDocuments(Result result) {
        result.reset();

        int len = result.getVisibleColumnCount();
        String[] fieldNames = new String[len];
        for (int i = 0; i < len; i++) {
            String columnName = result.getColumnName(i);
            if (Column.ROWID.equals(columnName)) {
                fieldNames[i] = "_id";
            } else {
                fieldNames[i] = columnName.toLowerCase();
            }
        }

        BsonArray documents = new BsonArray();
        while (result.next()) {
            BsonDocument document = toBsonDocument(fieldNames, result.currentRow());
            documents.add(document);
        }
        return documents;
    }
}

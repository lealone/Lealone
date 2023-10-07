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
import org.lealone.db.table.Table;
import org.lealone.docdb.server.DocDBServerConnection;
import org.lealone.docdb.server.DocDBTask;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.query.Select;

public class BCAggregate extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            DocDBServerConnection conn, DocDBTask task) {
        Table table = findTable(doc, "aggregate", conn);
        if (table != null) {
            aggregate(doc, conn, table, task);
            return null;
        } else {
            return createResponseDocument(doc, 0);
        }
    }

    private static BsonDocument createResponseDocument(BsonDocument doc, int rowCount) {
        BsonDocument document = new BsonDocument();
        BsonDocument cursor = new BsonDocument();
        append(cursor, "id", 0L);
        append(cursor, "ns",
                doc.getString("$db").getValue() + "." + doc.getString("aggregate").getValue());
        BsonArray documents = new BsonArray();
        BsonDocument agg = new BsonDocument();
        append(agg, "_id", 1);
        append(agg, "n", rowCount);
        documents.add(agg);
        cursor.append("firstBatch", documents);
        document.append("cursor", cursor);
        setOk(document);
        return document;
    }

    private static void aggregate(BsonDocument doc, DocDBServerConnection conn, Table table,
            DocDBTask task) {
        ServerSession session = task.session;
        Select select = new Select(session);
        TableFilter tableFilter = new TableFilter(session, table, null, true, select);
        select.addTableFilter(tableFilter, true);
        ArrayList<Expression> selectExpressions = Utils.newSmallArrayList();

        BsonArray pipeline = doc.getArray("pipeline", null);
        if (pipeline != null) {
            for (int i = 0, size = pipeline.size(); i < size; i++) {
                BsonDocument document = pipeline.get(i).asDocument();
                BsonDocument group = document.getDocument("$group", null);
                if (group != null) {
                    BsonDocument agg = group.getDocument("n", null);
                    if (agg != null && agg.containsKey("$sum")) {
                        Expression e = Aggregate.create(Aggregate.COUNT_ALL, null, select, false);
                        selectExpressions.add(e);
                        select.setGroupQuery();
                        break;
                    }
                }
            }
        }

        select.setExpressions(selectExpressions);
        select.init();
        select.prepare();

        PreparedSQLStatement.Yieldable<?> yieldable = select.createYieldableQuery(-1, false, ar -> {
            if (ar.isSucceeded()) {
                Result result = ar.getResult();
                result.next();
                task.conn.sendResponse(task.requestId,
                        createResponseDocument(task.doc, result.currentRow()[0].getInt()));
            } else {
                task.conn.sendError(task.session, task.requestId, ar.getCause());
            }
        });
        task.si.submitYieldableCommand(task.requestId, yieldable);
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.util.Utils;
import org.lealone.db.index.Cursor;
import org.lealone.db.result.Result;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.ValueMap;
import org.lealone.docdb.server.DocDBServerConnection;
import org.lealone.docdb.server.DocDBTask;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.Wildcard;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.query.Select;

public class BCFind extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            DocDBServerConnection conn, DocDBTask task) {
        BsonArray documents = new BsonArray();
        Table table = findTable(doc, "find", conn);
        if (table != null) {
            find(doc, conn, table, documents, task);
            return null;
        } else {
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

    private static ExpressionColumn getExpressionColumn(TableFilter tableFilter, String columnName) {
        return new ExpressionColumn(tableFilter.getTable().getDatabase(), tableFilter.getSchemaName(),
                tableFilter.getTableAlias(), columnName);
    }

    private static void find(BsonDocument doc, DocDBServerConnection conn, Table table,
            BsonArray documents, DocDBTask task) {
        ServerSession session = task.session;
        Select select = new Select(session);
        TableFilter tableFilter = new TableFilter(session, table, null, true, select);
        select.addTableFilter(tableFilter, true);
        BsonDocument filter = doc.getDocument("filter", null);
        if (filter != null) {
            if (DEBUG)
                logger.info("filter: {}", filter.toJson());
            filter.forEach((k, v) -> {
                String columnName = k.toUpperCase();
                if ("_ID".equals(columnName)) {
                    columnName = Column.ROWID;
                }
                Expression left = getExpressionColumn(tableFilter, columnName);
                Expression right = toValueExpression(v);
                Comparison cond = new Comparison(session, Comparison.EQUAL, left, right);
                select.addCondition(cond);
            });
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

                while (result.next()) {
                    BsonDocument document = toBsonDocument(fieldNames, result.currentRow());
                    documents.add(document);
                }
                task.conn.sendResponse(task.requestId, createResponseDocument(doc, documents));
            } else {
                task.conn.sendError(task.session, -1, ar.getCause());
            }
        });
        task.si.submitYieldableCommand(-1, yieldable);
    }

    @SuppressWarnings("unused")
    private static void findOld(BsonDocument doc, DocDBServerConnection conn, Table table,
            BsonArray documents) {
        ServerSession session = getSession(table.getDatabase(), conn);
        BsonDocument filter = doc.getDocument("filter", null);
        Long id = null;
        if (filter != null) {
            if (DEBUG)
                logger.info("filter: {}", filter.toJson());
            if (filter.size() == 1) {
                id = getId(filter);
            }
        }

        try {
            if (id != null) {
                Row row = table.getTemplateRow();
                row.setKey(id.longValue());
                Cursor cursor = table.getScanIndex(session).find(session, row, row);
                if (cursor.next()) {
                    BsonDocument document = toBsonDocument((ValueMap) cursor.get().getValue(0));
                    documents.add(document);
                }
            } else {
                Cursor cursor = table.getScanIndex(session).find(session, null, null);
                while (cursor.next()) {
                    BsonDocument document = toBsonDocument((ValueMap) cursor.get().getValue(0));
                    if (filter != null) {
                        boolean b = true;
                        for (Entry<String, BsonValue> e : filter.entrySet()) {
                            BsonValue v = document.get(e.getKey());
                            if (v == null || !v.equals(e.getValue())) {
                                b = false;
                                break;
                            }
                        }
                        if (b) {
                            documents.add(document);
                        }
                    } else {
                        documents.add(document);
                    }
                }
            }
            session.commit();
        } finally {
            session.close();
        }
    }
}

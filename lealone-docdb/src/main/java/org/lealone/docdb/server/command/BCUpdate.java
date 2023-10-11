/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.docdb.server.DocDBServerConnection;
import org.lealone.docdb.server.DocDBTask;
import org.lealone.sql.dml.Update;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.Operation;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.expression.function.Function;
import org.lealone.sql.optimizer.TableFilter;

public class BCUpdate extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            DocDBServerConnection conn, DocDBTask task) {
        Table table = findTable(doc, "update", conn);
        if (table != null) {
            update(input, doc, conn, table, task);
            return null;
        } else {
            return createResponseDocument(0);
        }
    }

    private static void update(ByteBufferBsonInput input, BsonDocument doc, DocDBServerConnection conn,
            Table table, DocDBTask task) {
        ServerSession session = task.session;
        Update update = new Update(session);
        TableFilter tableFilter = new TableFilter(session, table, null, true, null);
        update.setTableFilter(tableFilter);
        ArrayList<BsonDocument> updates = readPayload(input, doc, conn, "updates");
        for (BsonDocument updateDoc : updates) {
            BsonDocument q = updateDoc.getDocument("q", null);
            BsonDocument u = updateDoc.getDocument("u", null);
            if (q != null) {
                update.setCondition(toWhereCondition(q, tableFilter, session));
            }
            if (u != null) {
                setAssignment(u, tableFilter, session, table, update);
            }
        }
        update.prepare();
        createAndSubmitYieldableUpdate(task, update);
    }

    private static void setOperation(BsonDocument doc, TableFilter tableFilter, Table table,
            Update update, int opType) {
        setAssignment(doc, tableFilter, null, table, update, e -> {
            ExpressionColumn left = getExpressionColumn(tableFilter, e.getKey().toUpperCase());
            return new Operation(opType, left, toValueExpression(e.getValue()));
        });
    }

    private static void setAssignment(BsonDocument doc, TableFilter tableFilter, ServerSession session,
            Table table, Update update,
            java.util.function.Function<Entry<String, BsonValue>, Expression> function) {
        for (Entry<String, BsonValue> e : doc.entrySet()) {
            Column column = parseColumn(table, e.getKey());
            Expression expression = function.apply(e);
            update.setAssignment(column, expression);
        }
    }

    private static void setMinMax(BsonDocument doc, TableFilter tableFilter, ServerSession session,
            Table table, Update update, int compareType) {
        setAssignment(doc, tableFilter, session, table, update, e -> {
            ExpressionColumn col = getExpressionColumn(tableFilter, e.getKey().toUpperCase());
            Expression v = toValueExpression(e.getValue());
            Comparison c = new Comparison(session, compareType, v, col);
            Function f = Function.getFunction(session.getDatabase(), "CASEWHEN");
            f.setParameter(0, c);
            f.setParameter(1, v);
            f.setParameter(2, col);
            f.doneWithParameters();
            return f;
        });
    }

    private static void setAssignment(BsonDocument u, TableFilter tableFilter, ServerSession session,
            Table table, Update update) {
        for (Entry<String, BsonValue> e : u.entrySet()) {
            String k = e.getKey();
            BsonDocument doc = e.getValue().asDocument();
            switch (k) {
            // Field Update Operators
            case "$set":
            case "$setOnInsert":
                setAssignment(doc, tableFilter, session, table, update, e2 -> {
                    return toValueExpression(e2.getValue());
                });
                break;
            case "$currentDate":
                setAssignment(doc, tableFilter, session, table, update, e2 -> {
                    return Function.getFunction(session.getDatabase(), "CURRENT_DATE");
                });
                break;
            case "$inc":
                setOperation(doc, tableFilter, table, update, Operation.PLUS);
                break;
            case "$mul":
                setOperation(doc, tableFilter, table, update, Operation.MULTIPLY);
                break;
            case "$min":
                setMinMax(doc, tableFilter, session, table, update, Comparison.SMALLER);
                break;
            case "$max":
                setMinMax(doc, tableFilter, session, table, update, Comparison.BIGGER);
                break;
            case "$rename":
                for (Entry<String, BsonValue> e2 : doc.entrySet()) {
                    String oldName = e2.getKey();
                    String newName = e2.getValue().asString().getValue();
                    String sql = "ALTER TABLE " + table.getName() + " ALTER COLUMN " + oldName
                            + " RENAME TO " + newName;
                    session.prepareStatementLocal(sql).executeUpdate();
                }
                break;
            case "$unset":
                setAssignment(doc, tableFilter, session, table, update, e2 -> {
                    return ValueExpression.getNull();
                });
                break;
            // Bitwise Update Operator
            case "$bit":
                break;
            // Array Update Operators
            case "$addToSet":
                break;
            case "$pop":
                break;
            case "$pull":
                break;
            case "$push":
                break;
            case "$pullAll":
                break;
            case "$each":
                break;
            case "$position":
                break;
            case "$slice":
                break;
            case "$sort":
                break;
            default:
                throw DbException.getUnsupportedException("update operator " + k);
            }
        }
    }
}

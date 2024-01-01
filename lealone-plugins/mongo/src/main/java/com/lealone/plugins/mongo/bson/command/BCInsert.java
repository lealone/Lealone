/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mongo.bson.command;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.io.ByteBufferBsonInput;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.StatementBuilder;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;
import com.lealone.plugins.mongo.server.MongoServerConnection;
import com.lealone.plugins.mongo.server.MongoTask;
import com.lealone.sql.dml.Insert;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ValueExpression;

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
        outer: for (int i = 0; i < size; i++) {
            Expression values[] = new Expression[table.getColumns().length];
            HashSet<Column> set = new HashSet<>();
            BsonDocument document = documents.get(i);
            for (Entry<String, BsonValue> e : document.entrySet()) {
                String columnName = e.getKey();
                BsonValue v = e.getValue();
                Column column = parseColumn(table, columnName);
                if (column == null) {
                    // 如果后续写入的字段不存在，动态增加新的
                    addColumn(session, table, columnName, v);
                    insert.clearRows();
                    i = -1;
                    continue outer;
                }
                if (!set.add(column)) {
                    throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getSQL());
                }
                try {
                    Value columnValuue = toValue(v);
                    columnValuue = column.convert(columnValuue);
                    values[column.getColumnId()] = ValueExpression.get(columnValuue);
                } catch (Throwable t) {
                    // 如果后续写入的字段值的类型跟字段的类型不匹配，将字段的类型改成通用的varchar类型
                    alterColumnType(session, table, columnName);
                    insert.clearRows();
                    i = -1;
                    continue outer;
                }
            }
            insert.addRow(values);
        }
        insert.setColumns(table.getColumns());
        insert.prepare();
        createAndSubmitYieldableUpdate(task, insert);
    }

    private static void addColumn(ServerSession session, Table table, String columnName, BsonValue v) {
        StatementBuilder sql = new StatementBuilder();
        sql.append("ALTER TABLE ").append(table.getName()).append(" ADD COLUMN ").append(columnName)
                .append(" ");
        appendColumnType(sql, v);
        session.executeUpdateLocal(sql.toString());
    }

    private static void alterColumnType(ServerSession session, Table table, String columnName) {
        StatementBuilder sql = new StatementBuilder();
        sql.append("ALTER TABLE ").append(table.getName()).append(" ALTER COLUMN ").append(columnName)
                .append(" varchar");
        session.executeUpdateLocal(sql.toString());
    }
}

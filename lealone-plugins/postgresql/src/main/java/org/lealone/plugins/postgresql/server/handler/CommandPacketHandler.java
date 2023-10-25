/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.server.handler;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.List;

import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.ScriptReader;
import org.lealone.common.util.StringUtils;
import org.lealone.db.CommandParameter;
import org.lealone.db.Database;
import org.lealone.db.result.Result;
import org.lealone.db.schema.Schema;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.plugins.postgresql.server.PgServer;
import org.lealone.plugins.postgresql.server.PgServerConnection;
import org.lealone.plugins.postgresql.server.PgType;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;

// 官方协议文档: https://www.postgresql.org/docs/15/protocol-message-formats.html
public class CommandPacketHandler extends PacketHandler {

    private final CaseInsensitiveMap<Prepared> prepared = new CaseInsensitiveMap<>();
    private final CaseInsensitiveMap<Portal> portals = new CaseInsensitiveMap<>();

    public CommandPacketHandler(PgServer server, PgServerConnection conn) {
        super(server, conn);
    }

    @Override
    public void handle(int x) throws IOException {
        isQuery = false;
        switchBlock: switch (x) {
        case 'P': {
            server.trace("Parse");
            Prepared p = new Prepared();
            p.name = readString();
            p.sql = getSQL(readString());
            int paramTypesCount = readShort();
            int[] paramTypes = null;
            if (paramTypesCount > 0) {
                paramTypes = new int[paramTypesCount];
                for (int i = 0; i < paramTypesCount; i++) {
                    int type = readInt();
                    server.checkType(type);
                    paramTypes[i] = type;
                }
            }
            try {
                p.prep = session.prepareStatementLocal(p.sql);
                List<? extends CommandParameter> parameters = p.prep.getParameters();
                int count = parameters.size();
                p.paramTypes = new int[count];
                for (int i = 0; i < count; i++) {
                    int type;
                    if (i < paramTypesCount && paramTypes[i] != 0) {
                        type = paramTypes[i];
                        server.checkType(type);
                    } else {
                        type = PgType.convertType(parameters.get(i).getType());
                    }
                    p.paramTypes[i] = type;
                }
                prepared.put(p.name, p);
                sendParseComplete();
            } catch (Exception e) {
                sendErrorResponse(e);
            }
            break;
        }
        case 'B': {
            server.trace("Bind");
            Portal portal = new Portal();
            portal.name = readString();
            String prepName = readString();
            Prepared prep = prepared.get(prepName);
            if (prep == null) {
                sendErrorResponse("Prepared not found");
                break;
            }
            portal.prep = prep;
            portals.put(portal.name, portal);
            int formatCodeCount = readShort();
            int[] formatCodes = new int[formatCodeCount];
            for (int i = 0; i < formatCodeCount; i++) {
                formatCodes[i] = readShort();
            }
            int paramCount = readShort();
            for (int i = 0; i < paramCount; i++) {
                int paramLen = readInt();
                byte[] d2 = DataUtils.newBytes(paramLen);
                readFully(d2);
                try {
                    setParameter(prep.prep, i, d2, formatCodes);
                } catch (Exception e) {
                    sendErrorResponse(e);
                    break switchBlock;
                }
            }
            int resultCodeCount = readShort();
            portal.resultColumnFormat = new int[resultCodeCount];
            for (int i = 0; i < resultCodeCount; i++) {
                portal.resultColumnFormat[i] = readShort();
            }
            sendBindComplete();
            break;
        }
        case 'C': {
            char type = (char) readByte();
            String name = readString();
            server.trace("Close");
            if (type == 'S') {
                Prepared p = prepared.remove(name);
                if (p != null) {
                    p.prep.close();
                }
            } else if (type == 'P') {
                portals.remove(name);
            } else {
                server.trace("expected S or P, got " + type);
                sendErrorResponse("expected S or P");
                break;
            }
            sendCloseComplete();
            break;
        }
        case 'D': {
            char type = (char) readByte();
            String name = readString();
            server.trace("Describe");
            if (type == 'S') {
                Prepared p = prepared.get(name);
                if (p == null) {
                    sendErrorResponse("Prepared not found: " + name);
                } else {
                    sendParameterDescription(p);
                }
            } else if (type == 'P') {
                Portal p = portals.get(name);
                if (p == null) {
                    sendErrorResponse("Portal not found: " + name);
                } else {
                    try {
                        sendRowDescription(p.prep.prep.getMetaData(), p.resultColumnFormat);
                    } catch (Exception e) {
                        sendErrorResponse(e);
                    }
                }
            } else {
                server.trace("expected S or P, got " + type);
                sendErrorResponse("expected S or P");
            }
            break;
        }
        case 'E': {
            String name = readString();
            server.trace("Execute");
            Portal p = portals.get(name);
            if (p == null) {
                sendErrorResponse("Portal not found: " + name);
                break;
            }
            int maxRows = readShort();
            Prepared prepared = p.prep;
            PreparedSQLStatement prep = prepared.prep;
            server.trace(prepared.sql);
            try {
                submitYieldableCommand(prep, maxRows, false); // 不需要发送RowDescription
            } catch (Exception e) {
                sendErrorResponse(e);
            }
            break;
        }
        case 'S': {
            server.trace("Sync");
            sendReadyForQuery();
            break;
        }
        case 'Q': {
            isQuery = true;
            server.trace("Query");
            String query = readString();
            ScriptReader reader = new ScriptReader(new StringReader(query));
            while (true) {
                PreparedSQLStatement prep = null;
                try {
                    String s = reader.readStatement();
                    if (s == null) {
                        break;
                    }
                    s = getSQL(s);
                    prep = session.prepareStatement(s);
                    submitYieldableCommand(prep, -1, true);
                } catch (Exception e) {
                    sendErrorResponse(e);
                    break;
                }
            }
            isQuery = false;
            sendReadyForQuery();
            break;
        }
        case 'X': {
            server.trace("Terminate");
            conn.close();
            break;
        }
        default:
            server.trace("Unsupported: " + x + " (" + (char) x + ")");
            break;
        }
    }

    // 异步执行SQL语句
    private void submitYieldableCommand(PreparedSQLStatement stmt, int maxRows,
            boolean sendRowDescription) {
        PreparedSQLStatement.Yieldable<?> yieldable;
        if (stmt.isQuery()) {
            yieldable = stmt.createYieldableQuery(maxRows, false, ar -> {
                if (ar.isSucceeded()) {
                    try {
                        Result result = ar.getResult();
                        if (sendRowDescription)
                            sendRowDescription(stmt.getMetaData(), null);
                        while (result.next()) {
                            sendDataRow(result);
                        }
                        sendCommandComplete(stmt, 0);
                    } catch (Exception e) {
                        sendErrorResponse(e);
                    }
                } else {
                    sendErrorResponse(ar.getCause());
                }
            });
        } else {
            yieldable = stmt.createYieldableUpdate(ar -> {
                if (ar.isSucceeded()) {
                    try {
                        sendCommandComplete(stmt, ar.getResult());
                    } catch (Exception e) {
                        sendErrorResponse(e);
                    }
                } else {
                    sendErrorResponse(ar.getCause());
                }
            });
        }
        si.submitYieldableCommand(0, yieldable);
    }

    private String getSQL(String s) {
        String lower = StringUtils.toLowerEnglish(s);
        if (lower.startsWith("show max_identifier_length")) {
            s = "CALL 63";
        } else if (lower.startsWith("set client_encoding to")) {
            s = "set DATESTYLE ISO";
        }
        // s = StringUtils.replaceAll(s, "i.indkey[ia.attnum-1]", "0");
        if (server.getTrace()) {
            server.trace(s + ";");
        }
        return s;
    }

    private void sendCommandComplete(PreparedSQLStatement stat, int updateCount) throws IOException {
        startMessage('C');
        switch (stat.getType()) {
        case SQLStatement.INSERT:
            writeStringPart("CommandInterfaceINSERT 0 ");
            writeString(Integer.toString(updateCount));
            break;
        case SQLStatement.UPDATE:
            writeStringPart("UPDATE ");
            writeString(Integer.toString(updateCount));
            break;
        case SQLStatement.DELETE:
            writeStringPart("DELETE ");
            writeString(Integer.toString(updateCount));
            break;
        case SQLStatement.SELECT:
        case SQLStatement.CALL:
            writeString("SELECT");
            break;
        case SQLStatement.BEGIN:
            writeString("BEGIN");
            break;
        default:
            server.trace("check CommandComplete tag for command " + stat);
            writeStringPart("UPDATE ");
            writeString(Integer.toString(updateCount));
        }
        sendMessage();
    }

    private void sendDataRow(Result rs) throws Exception {
        int columns = rs.getVisibleColumnCount();
        String[] values = new String[columns];
        for (int i = 0; i < columns; i++) {
            values[i] = rs.currentRow()[i].getString();
        }
        startMessage('D');
        writeShort(columns);
        for (String s : values) {
            if (s == null) {
                writeInt(-1);
            } else {
                // TODO write Binary data
                byte[] d2 = s.getBytes(getEncoding());
                writeInt(d2.length);
                write(d2);
            }
        }
        sendMessage();
    }

    private void setParameter(PreparedSQLStatement prep, int i, byte[] d2, int[] formatCodes)
            throws SQLException {
        boolean text = (i >= formatCodes.length) || (formatCodes[i] == 0);
        String s;
        try {
            if (text) {
                s = new String(d2, getEncoding());
            } else {
                server.trace("Binary format not supported");
                s = new String(d2, getEncoding());
            }
        } catch (Exception e) {
            server.traceError(e);
            s = null;
        }
        prep.getParameters().get(i).setValue(ValueString.get(s));
    }

    private void sendParameterDescription(Prepared p) throws IOException {
        try {
            List<? extends CommandParameter> parameters = p.prep.getParameters();
            int count = parameters.size();
            startMessage('t');
            writeShort(count);
            for (int i = 0; i < count; i++) {
                int type;
                if (p.paramTypes != null && p.paramTypes[i] != 0) {
                    type = p.paramTypes[i];
                } else {
                    type = PgType.PG_TYPE_VARCHAR;
                }
                server.checkType(type);
                writeInt(type);
            }
            sendMessage();
        } catch (Exception e) {
            sendErrorResponse(e);
        }
    }

    private void sendNoData() throws IOException {
        startMessage('n');
        sendMessage();
    }

    private void sendRowDescription(Result result, int[] formatCodes) throws IOException {
        if (result == null) {
            sendNoData();
        } else {
            int columns = result.getVisibleColumnCount();
            int[] oids = new int[columns];
            int[] attnums = new int[columns];
            int[] types = new int[columns];
            int[] precision = new int[columns];
            String[] names = new String[columns];
            Database database = session.getDatabase();
            for (int i = 0; i < columns; i++) {
                String name = result.getColumnName(i);
                Schema schema = database.findSchema(session, result.getSchemaName(i));
                if (schema != null) {
                    Table table = schema.findTableOrView(session, result.getTableName(i));
                    if (table != null) {
                        oids[i] = table.getId();
                        Column column = table.getColumn(name);
                        if (column != null) {
                            attnums[i] = column.getColumnId() + 1;
                        }
                    }
                }
                names[i] = name;
                int type = result.getColumnType(i);
                int pgType = PgType.convertValueType(type);
                // the ODBC client needs the column pg_catalog.pg_index
                // to be of type 'int2vector'
                // if (name.equalsIgnoreCase("indkey") &&
                // "pg_index".equalsIgnoreCase(
                // meta.getTableName(i + 1))) {
                // type = PgServer.PG_TYPE_INT2VECTOR;
                // }
                precision[i] = result.getDisplaySize(i);
                if (type != Value.NULL) {
                    server.checkType(pgType);
                }
                types[i] = pgType;
            }
            startMessage('T');
            writeShort(columns);
            for (int i = 0; i < columns; i++) {
                writeString(StringUtils.toLowerEnglish(names[i]));
                // object ID
                writeInt(oids[i]);
                // attribute number of the column
                writeShort(attnums[i]);
                // data type
                writeInt(types[i]);
                // pg_type.typlen
                writeShort(getTypeSize(types[i], precision[i]));
                // pg_attribute.atttypmod
                writeInt(-1);
                // the format type: text = 0, binary = 1
                writeShort(formatAsText(types[i], formatCodes, i) ? 0 : 1);
            }
            sendMessage();
        }
    }

    /**
     * Check whether the given type should be formatted as text.
     *
     * @param pgType data type
     * @param formatCodes format codes, or {@code null}
     * @param column 0-based column number
     * @return true for text
     */
    private static boolean formatAsText(int pgType, int[] formatCodes, int column) {
        boolean text = true;
        if (formatCodes != null && formatCodes.length > 0) {
            if (formatCodes.length == 1) {
                text = formatCodes[0] == 0;
            } else if (column < formatCodes.length) {
                text = formatCodes[column] == 0;
            }
        }
        return text;
    }

    private static int getTypeSize(int pgType, int precision) {
        switch (pgType) {
        case PgType.PG_TYPE_VARCHAR:
            return Math.max(255, precision + 10);
        default:
            return precision + 4;
        }
    }

    private void sendParseComplete() throws IOException {
        startMessage('1');
        sendMessage();
    }

    private void sendBindComplete() throws IOException {
        startMessage('2');
        sendMessage();
    }

    private void sendCloseComplete() throws IOException {
        startMessage('3');
        sendMessage();
    }

    /**
     * Represents a PostgreSQL Prepared object.
     */
    private static class Prepared {

        /**
         * The object name.
         */
        String name;

        /**
         * The SQL statement.
         */
        String sql;

        /**
         * The prepared statement.
         */
        PreparedSQLStatement prep;

        /**
         * The list of parameter types (if set).
         */
        int[] paramTypes;
    }

    /**
     * Represents a PostgreSQL Portal object.
     */
    private static class Portal {

        /**
         * The portal name.
         */
        String name;

        /**
         * The format used in the result set columns (if set).
         */
        int[] resultColumnFormat;

        /**
         * The prepared object.
         */
        Prepared prep;
    }
}

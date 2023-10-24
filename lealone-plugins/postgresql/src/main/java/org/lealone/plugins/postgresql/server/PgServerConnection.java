/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.ScriptReader;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.CommandParameter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.PluginManager;
import org.lealone.db.result.Result;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.net.NetBuffer;
import org.lealone.net.WritableChannel;
import org.lealone.plugins.postgresql.io.NetBufferInput;
import org.lealone.plugins.postgresql.io.NetBufferOutput;
import org.lealone.server.AsyncServerConnection;
import org.lealone.server.Scheduler;
import org.lealone.server.SessionInfo;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLEngine;
import org.lealone.sql.SQLStatement;

/**
 * One server connection is opened for each client.
 * 
 * This class implements a subset of the PostgreSQL protocol as described here:
 * http://developer.postgresql.org/pgdocs/postgres/protocol.html
 * The PostgreSQL catalog is described here:
 * http://www.postgresql.org/docs/7.4/static/catalogs.html
 * 
 * @author H2 Group
 * @author zhh
 */
// 最新官方协议文档: https://www.postgresql.org/docs/15/protocol-message-formats.html
public class PgServerConnection extends AsyncServerConnection {

    private static final Logger logger = LoggerFactory.getLogger(PgServerConnection.class);

    private static final int BUFFER_SIZE = 4 * 1024;

    private final PgServer server;
    private final Scheduler scheduler;
    private ServerSession session;
    private SessionInfo si;
    private boolean stop;
    private NetBufferInput in;
    private NetBufferOutput out;
    private boolean initDone;
    private String userName;
    private String databaseName;
    private int processId;
    private String clientEncoding = Utils.getProperty("pgClientEncoding", "UTF-8");
    private String dateStyle = "ISO";
    private final CaseInsensitiveMap<Prepared> prepared = new CaseInsensitiveMap<>();
    private final CaseInsensitiveMap<Portal> portals = new CaseInsensitiveMap<>();
    private final ArrayList<NetBufferOutput> outList = new ArrayList<>();
    private boolean isQuery;

    protected PgServerConnection(PgServer server, WritableChannel writableChannel, Scheduler scheduler) {
        super(writableChannel, true);
        this.server = server;
        this.scheduler = scheduler;
    }

    @Override
    public void closeSession(SessionInfo si) {
    }

    @Override
    public int getSessionCount() {
        return 1;
    }

    private String readString() throws IOException {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        while (true) {
            int x = in.read();
            if (x <= 0) {
                break;
            }
            buff.write(x);
        }
        return new String(buff.toByteArray(), getEncoding());
    }

    private int readInt() {
        return in.readInt();
    }

    private int readShort() {
        return in.readShort();
    }

    private byte readByte() {
        return in.readByte();
    }

    private void readFully(byte[] buff) {
        in.readFully(buff);
    }

    private void createSession(String password) {
        Properties info = new Properties();
        info.put("MODE", PgServerEngine.NAME);
        info.put("USER", userName);
        info.put("PASSWORD", password);
        info.put("DEFAULT_SQL_ENGINE", PgServerEngine.NAME);
        String url = Constants.URL_PREFIX + Constants.URL_TCP + server.getHost() + ":" + server.getPort()
                + "/" + databaseName;
        ConnectionInfo ci = new ConnectionInfo(url, info);
        ci.setRemote(false);
        session = (ServerSession) ci.createSession();
        si = new SessionInfo(scheduler, this, session, -1, -1);
        scheduler.addSessionInfo(si);
        session.setSQLEngine(PluginManager.getPlugin(SQLEngine.class, PgServerEngine.NAME));
    }

    private void process(int x) throws IOException {
        isQuery = false;
        switchBlock: switch (x) {
        case 0:
            server.trace("Init");
            int version = readInt();
            if (version == 80877102) {
                server.trace("CancelRequest (not supported)");
                server.trace(" pid: " + readInt());
                server.trace(" key: " + readInt());
            } else if (version == 80877103) {
                server.trace("SSLRequest");
                out.write('N');
                out.flush();
            } else {
                server.trace("StartupMessage");
                server.trace(
                        " version " + version + " (" + (version >> 16) + "." + (version & 0xff) + ")");
                while (true) {
                    String param = readString();
                    if (param.length() == 0) {
                        break;
                    }
                    String value = readString();
                    if ("user".equals(param)) {
                        this.userName = value;
                    } else if ("database".equals(param)) {
                        this.databaseName = value;
                    } else if ("client_encoding".equals(param)) {
                        // UTF8
                        clientEncoding = value;
                    } else if ("DateStyle".equals(param)) {
                        dateStyle = value;
                    }
                    // extra_float_digits 2
                    // geqo on (Genetic Query Optimization)
                    server.trace(" param " + param + "=" + value);
                }
                initDone = true;
                sendAuthenticationCleartextPassword();
            }
            break;
        case 'p': {
            server.trace("PasswordMessage");
            String password = readString();
            try {
                createSession(password);
                initDb();
                sendAuthenticationOk();
            } catch (Exception e) {
                sendErrorResponse(e);
                stop = true;
            }
            break;
        }
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
            close();
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

    private String getEncoding() {
        if ("UNICODE".equals(clientEncoding)) {
            return "UTF-8";
        }
        return clientEncoding;
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

    private void sendErrorResponse(Throwable re) {
        SQLException e = DbException.toSQLException(re);
        server.traceError(e);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString(e.getSQLState());
        write('M');
        writeString(e.getMessage());
        write('D');
        writeString(e.toString());
        write(0);
        sendMessage();
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

    private void sendErrorResponse(String message) throws IOException {
        server.trace("Exception: " + message);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        // PROTOCOL VIOLATION
        writeString("08P01");
        write('M');
        writeString(message);
        sendMessage();
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

    private void initDb() throws SQLException {
        synchronized (server) {
            // better would be: set the database to exclusive mode
            // rs = conn.getMetaData().getTables(null, "PG_CATALOG", "PG_VERSION", null);
            Schema schema = session.getDatabase().findSchema(session, "PG_CATALOG");
            if (schema == null || schema.getTableOrView(session, "PG_VERSION") != null) {
                PgServer.installPgCatalog(session, false);
            }
            Result r = session.prepareStatementLocal("SELECT * FROM PG_CATALOG.PG_VERSION")
                    .executeQuery(-1).get();
            if (!r.next() || r.currentRow()[0].getInt() < 2) {
                // installation incomplete, or old version
                PgServer.installPgCatalog(session, false);
            } else {
                // version 2 or newer: check the read version
                int versionRead = r.currentRow()[1].getInt();
                if (versionRead > 2) {
                    throw DbException.throwInternalError("Incompatible PG_VERSION");
                }
            }
            r.close();
        }
        session.prepareStatementLocal("set search_path = PUBLIC, pg_catalog").executeUpdate().get();
        HashSet<Integer> typeSet = server.getTypeSet();
        if (typeSet.isEmpty()) {
            Result r = session.prepareStatementLocal("SELECT OID FROM PG_CATALOG.PG_TYPE")
                    .executeQuery(-1).get();
            while (r.next()) {
                typeSet.add(r.currentRow()[0].getInt());
            }
            r.close();
        }
    }

    /**
     * Close this connection.
     */
    @Override
    public void close() {
        if (session == null)
            return;
        try {
            stop = true;
            session.close();
            server.trace("Close");
            super.close();
        } catch (Exception e) {
            server.traceError(e);
        }
        session = null;
        server.removeConnection(this);
    }

    private void sendAuthenticationCleartextPassword() throws IOException {
        startMessage('R');
        writeInt(3);
        sendMessage();
    }

    private void sendAuthenticationOk() throws IOException {
        startMessage('R');
        writeInt(0);
        sendMessage();
        sendParameterStatus("client_encoding", clientEncoding);
        sendParameterStatus("DateStyle", dateStyle);
        sendParameterStatus("integer_datetimes", "off");
        sendParameterStatus("is_superuser", "off");
        sendParameterStatus("server_encoding", "SQL_ASCII");
        sendParameterStatus("server_version", PgServer.PG_VERSION);
        sendParameterStatus("session_authorization", userName);
        sendParameterStatus("standard_conforming_strings", "off");
        // TODO PostgreSQL TimeZone
        sendParameterStatus("TimeZone", "CET");
        sendBackendKeyData();
        sendReadyForQuery();
    }

    private void sendReadyForQuery() throws IOException {
        startMessage('Z');
        char c;
        if (session.isAutoCommit()) {
            // idle
            c = 'I';
        } else {
            // in a transaction block
            c = 'T';
        }
        write((byte) c);
        sendMessage();
    }

    private void sendBackendKeyData() {
        startMessage('K');
        writeInt(processId);
        writeInt(processId);
        sendMessage();
    }

    private void writeString(String s) {
        writeStringPart(s);
        write(0);
    }

    private void writeStringPart(String s) {
        try {
            write(s.getBytes(getEncoding()));
        } catch (UnsupportedEncodingException e) {
            throw DbException.convert(e);
        }
    }

    private void writeInt(int i) {
        out.writeInt(i);
    }

    private void writeShort(int i) {
        out.writeShort(i);
    }

    private void write(byte[] data) {
        out.write(data);
    }

    private void write(int b) {
        out.write(b);
    }

    private void startMessage(int newMessageType) {
        out.write(newMessageType);
        out.writeInt(0); // 占位
    }

    private void sendMessage() {
        out.setInt(1, out.length() - 1); // 回填
        if (isQuery) {
            outList.add(out);
            out = new NetBufferOutput(writableChannel, BUFFER_SIZE, scheduler.getDataBufferFactory());
        } else {
            if (!outList.isEmpty()) {
                for (NetBufferOutput o : outList)
                    o.flush();
                outList.clear();
            }
            out.flush();
        }
    }

    private void sendParameterStatus(String param, String value) {
        startMessage('S');
        writeString(param);
        writeString(value);
        sendMessage();
    }

    void setProcessId(int id) {
        this.processId = id;
    }

    /**
     * Represents a PostgreSQL Prepared object.
     */
    static class Prepared {

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
    static class Portal {

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

    private final ByteBuffer packetLengthByteBufferInitDone = ByteBuffer.allocateDirect(5);

    @Override
    public ByteBuffer getPacketLengthByteBuffer() {
        if (initDone)
            return packetLengthByteBufferInitDone;
        else
            return packetLengthByteBuffer;
    }

    @Override
    public int getPacketLength() {
        int len;
        if (initDone) {
            packetLengthByteBufferInitDone.get();
            len = packetLengthByteBufferInitDone.getInt();
            packetLengthByteBufferInitDone.flip();
        } else {
            len = packetLengthByteBuffer.getInt();
            packetLengthByteBuffer.flip();
        }
        return len - 4;
    }

    @Override
    public void handle(NetBuffer buffer) {
        // postgresql执行一条sql要分成5个包: Parse Bind Describe Execute Sync
        // 执行到Execute这一步时会异步提交sql，此时不能继续处理Sync包，否则客户端会提前收到Sync的响应，但sql的结果还看不到
        if (si == null)
            handlePacket(buffer);
        else
            si.submitTask(new PgTask(this, buffer));
    }

    void handlePacket(NetBuffer buffer) {
        if (!buffer.isOnlyOnePacket()) {
            DbException.throwInternalError("NetBuffer must be OnlyOnePacket");
        }
        if (stop)
            return;
        out = new NetBufferOutput(writableChannel, BUFFER_SIZE, scheduler.getDataBufferFactory());
        in = new NetBufferInput(buffer);
        try {
            int x;
            if (initDone) {
                x = packetLengthByteBufferInitDone.get();
                packetLengthByteBufferInitDone.clear();
                if (x < 0) {
                    stop = true;
                    return;
                }
            } else {
                x = 0;
            }
            process(x);
            in.close();
        } catch (Exception e) {
            logger.error("Parse packet exception", e);
            try {
                sendErrorResponse(e);
            } catch (Exception e1) {
                logger.error("sendErrorResponse exception", e);
            }
        }
    }
}

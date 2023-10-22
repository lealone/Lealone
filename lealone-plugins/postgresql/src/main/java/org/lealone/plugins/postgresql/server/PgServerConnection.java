/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.lealone.client.jdbc.JdbcConnection;
import org.lealone.client.jdbc.JdbcPreparedStatement;
import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.JdbcUtils;
import org.lealone.common.util.ScriptReader;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetBuffer;
import org.lealone.net.WritableChannel;
import org.lealone.plugins.postgresql.io.NetBufferInput;
import org.lealone.plugins.postgresql.io.NetBufferOutput;
import org.lealone.server.Scheduler;
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
public class PgServerConnection extends AsyncConnection {

    private static final Logger logger = LoggerFactory.getLogger(PgServerConnection.class);

    private static final int BUFFER_SIZE = 4 * 1024;

    private final PgServer server;
    private final Scheduler scheduler;
    private Connection conn;
    private boolean stop;
    private NetBufferInput in;
    private NetBufferOutput out;
    private boolean initDone;
    private String userName;
    private String databaseName;
    private int processId;
    private String clientEncoding = Utils.getProperty("pgClientEncoding", "UTF-8");
    private String dateStyle = "ISO";
    private final HashMap<String, Prepared> prepared = new CaseInsensitiveMap<Prepared>();
    private final HashMap<String, Portal> portals = new CaseInsensitiveMap<Portal>();
    private final ArrayList<NetBufferOutput> outList = new ArrayList<>();
    private boolean isQuery;

    protected PgServerConnection(PgServer server, WritableChannel writableChannel, Scheduler scheduler) {
        super(writableChannel, true);
        this.server = server;
        this.scheduler = scheduler;
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

    private JdbcConnection createJdbcConnection(String password) throws SQLException {
        Properties info = new Properties();
        info.put("MODE", "PostgreSQL");
        info.put("USER", userName);
        info.put("PASSWORD", password);
        info.put("DEFAULT_SQL_ENGINE", PgServerEngine.NAME);
        String url = Constants.URL_PREFIX + Constants.URL_TCP + server.getHost() + ":" + server.getPort()
                + "/" + databaseName;
        ConnectionInfo ci = new ConnectionInfo(url, info);
        ci.setRemote(false);
        return new JdbcConnection(ci);
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
                conn = createJdbcConnection(password);
                // can not do this because when called inside
                // DriverManager.getConnection, a deadlock occurs
                // conn = DriverManager.getConnection(url, userName, password);
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
            int count = readShort();
            p.paramType = new int[count];
            for (int i = 0; i < count; i++) {
                int type = readInt();
                server.checkType(type);
                p.paramType[i] = type;
            }
            try {
                p.prep = (JdbcPreparedStatement) conn.prepareStatement(p.sql);
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
                    JdbcUtils.closeSilently(p.prep);
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
                    PreparedStatement prep = p.prep.prep;
                    try {
                        ResultSetMetaData meta = prep.getMetaData();
                        sendRowDescription(meta);
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
            JdbcPreparedStatement prep = prepared.prep;
            server.trace(prepared.sql);
            try {
                prep.setMaxRows(maxRows);
                boolean result = prep.execute();
                if (result) {
                    try {
                        ResultSet rs = prep.getResultSet();
                        // 不需要发送RowDescription
                        // ResultSetMetaData meta = rs.getMetaData();
                        // sendRowDescription(meta);
                        while (rs.next()) {
                            sendDataRow(rs);
                        }
                        sendCommandComplete(prep, 0);
                    } catch (Exception e) {
                        sendErrorResponse(e);
                    }
                } else {
                    sendCommandComplete(prep, prep.getUpdateCount());
                }
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
                JdbcStatement stat = null;
                try {
                    String s = reader.readStatement();
                    if (s == null) {
                        break;
                    }
                    s = getSQL(s);
                    stat = (JdbcStatement) conn.createStatement();
                    boolean result = stat.execute(s);
                    if (result) {
                        ResultSet rs = stat.getResultSet();
                        ResultSetMetaData meta = rs.getMetaData();
                        try {
                            sendRowDescription(meta);
                            while (rs.next()) {
                                sendDataRow(rs);
                            }
                            sendCommandComplete(stat, 0);
                        } catch (Exception e) {
                            sendErrorResponse(e);
                            break;
                        }
                    } else {
                        sendCommandComplete(stat, stat.getUpdateCount());
                    }
                } catch (SQLException e) {
                    sendErrorResponse(e);
                    break;
                } finally {
                    JdbcUtils.closeSilently(stat);
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

    private void sendCommandComplete(JdbcStatement stat, int updateCount) throws IOException {
        startMessage('C');
        switch (stat.getLastExecutedCommandType()) {
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

    private void sendDataRow(ResultSet rs) throws Exception {
        int columns = rs.getMetaData().getColumnCount();
        String[] values = new String[columns];
        for (int i = 0; i < columns; i++) {
            values[i] = rs.getString(i + 1);
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

    private void setParameter(PreparedStatement prep, int i, byte[] d2, int[] formatCodes)
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
        // if(server.getLog()) {
        // server.log(" " + i + ": " + s);
        // }
        prep.setString(i + 1, s);
    }

    private void sendErrorResponse(Exception re) throws IOException {
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
            PreparedStatement prep = p.prep;
            ParameterMetaData meta = prep.getParameterMetaData();
            int count = meta.getParameterCount();
            startMessage('t');
            writeShort(count);
            for (int i = 0; i < count; i++) {
                int type;
                if (p.paramType != null && p.paramType[i] != 0) {
                    type = p.paramType[i];
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

    private void sendRowDescription(ResultSetMetaData meta) throws Exception {
        if (meta == null) {
            sendNoData();
        } else {
            int columns = meta.getColumnCount();
            int[] types = new int[columns];
            int[] precision = new int[columns];
            String[] names = new String[columns];
            for (int i = 0; i < columns; i++) {
                String name = meta.getColumnName(i + 1);
                names[i] = name;
                int type = meta.getColumnType(i + 1);
                type = PgType.convertType(type);
                // the ODBC client needs the column pg_catalog.pg_index
                // to be of type 'int2vector'
                // if (name.equalsIgnoreCase("indkey") &&
                // "pg_index".equalsIgnoreCase(meta.getTableName(i + 1))) {
                // type = PgServer.PG_TYPE_INT2VECTOR;
                // }
                precision[i] = meta.getColumnDisplaySize(i + 1);
                server.checkType(type);
                types[i] = type;
            }
            startMessage('T');
            writeShort(columns);
            for (int i = 0; i < columns; i++) {
                writeString(StringUtils.toLowerEnglish(names[i]));
                // object ID
                writeInt(0);
                // attribute number of the column
                writeShort(0);
                // data type
                writeInt(types[i]);
                // pg_type.typlen
                writeShort(getTypeSize(types[i], precision[i]));
                // pg_attribute.atttypmod
                writeInt(-1);
                // text
                writeShort(0);
            }
            sendMessage();
        }
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
        Statement stat = null;
        ResultSet rs = null;
        try {
            synchronized (server) {
                // better would be: set the database to exclusive mode
                rs = conn.getMetaData().getTables(null, "PG_CATALOG", "PG_VERSION", null);
                boolean tableFound = rs.next();
                stat = conn.createStatement();
                if (!tableFound) {
                    installPgCatalog(stat);
                }
                JdbcUtils.closeSilently(rs);
                rs = stat.executeQuery("SELECT * FROM PG_CATALOG.PG_VERSION");
                if (!rs.next() || rs.getInt(1) < 2) {
                    // installation incomplete, or old version
                    installPgCatalog(stat);
                } else {
                    // version 2 or newer: check the read version
                    int versionRead = rs.getInt(2);
                    if (versionRead > 2) {
                        throw DbException.throwInternalError("Incompatible PG_VERSION");
                    }
                }
                JdbcUtils.closeSilently(rs);
            }
            stat.execute("set search_path = PUBLIC, pg_catalog");
            HashSet<Integer> typeSet = server.getTypeSet();
            if (typeSet.isEmpty()) {
                rs = stat.executeQuery("SELECT OID FROM PG_CATALOG.PG_TYPE");
                while (rs.next()) {
                    typeSet.add(rs.getInt(1));
                }
                JdbcUtils.closeSilently(rs);
            }
        } finally {
            JdbcUtils.closeSilently(stat);
        }
    }

    private static void installPgCatalog(Statement stat) throws SQLException {
        Reader r = null;
        try {
            r = Utils.getResourceAsReader(PgServer.PG_CATALOG_FILE);
            ScriptReader reader = new ScriptReader(r);
            while (true) {
                String sql = reader.readStatement();
                if (sql == null) {
                    break;
                }
                stat.execute(sql);
            }
            reader.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Can not read pg_catalog resource");
        } finally {
            IOUtils.closeSilently(r);
        }
    }

    /**
     * Close this connection.
     */
    @Override
    public void close() {
        if (conn == null)
            return;
        try {
            stop = true;
            JdbcUtils.closeSilently(conn);
            server.trace("Close");
            super.close();
        } catch (Exception e) {
            server.traceError(e);
        }
        conn = null;
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
        try {
            if (conn.getAutoCommit()) {
                // idle
                c = 'I';
            } else {
                // in a transaction block
                c = 'T';
            }
        } catch (SQLException e) {
            // failed transaction block
            c = 'E';
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
        JdbcPreparedStatement prep;

        /**
         * The list of parameter types (if set).
         */
        int[] paramType;
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

    private final ByteBuffer packetLengthByteBuffer = ByteBuffer.allocateDirect(4);
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
            } catch (IOException e1) {
                logger.error("sendErrorResponse exception", e);
            }
        }
    }
}

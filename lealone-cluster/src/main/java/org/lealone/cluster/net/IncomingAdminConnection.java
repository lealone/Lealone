/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.cluster.net;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.common.message.JdbcSQLException;
import org.lealone.common.util.SmallMap;
import org.lealone.common.util.StringUtils;
import org.lealone.db.CommandParameter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.result.Result;
import org.lealone.db.value.Transfer;
import org.lealone.db.value.Value;
import org.lealone.sql.PreparedStatement;

/**
 * One server thread is opened per client connection.
 * 
 * @author H2 Group
 * @author zhh
 */
public class IncomingAdminConnection extends Thread implements Closeable {

    private final SmallMap cache = new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private final Transfer transfer;

    private final Set<Closeable> group;
    private Session session;
    private boolean stop;
    private Thread thread;
    private int clientVersion;
    private String sessionId;

    protected IncomingAdminConnection(Socket socket, Set<Closeable> group) {
        transfer = new Transfer(null, socket);
        this.group = group;
    }

    @Override
    public void run() {
        try {
            transfer.init();
            // TODO server: should support a list of allowed databases
            // and a list of allowed clients
            try {
                int minClientVersion = transfer.readInt();
                if (minClientVersion < Constants.TCP_PROTOCOL_VERSION_MIN) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + minClientVersion, ""
                            + Constants.TCP_PROTOCOL_VERSION_MIN);
                } else if (minClientVersion > Constants.TCP_PROTOCOL_VERSION_MAX) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + minClientVersion, ""
                            + Constants.TCP_PROTOCOL_VERSION_MAX);
                }
                int maxClientVersion = transfer.readInt();
                if (maxClientVersion >= Constants.TCP_PROTOCOL_VERSION_MAX) {
                    clientVersion = Constants.TCP_PROTOCOL_VERSION_CURRENT;
                } else {
                    clientVersion = minClientVersion;
                }
                transfer.setVersion(clientVersion);
                String db = transfer.readString();
                String originalURL = transfer.readString();
                String userName = transfer.readString();
                userName = StringUtils.toUpperEnglish(userName);
                session = createSession(db, originalURL, userName, transfer);
                transfer.setSession(session);
                transfer.writeInt(Session.STATUS_OK);
                transfer.writeInt(clientVersion);
                transfer.flush();
            } catch (Throwable e) {
                sendError(e);
                stop = true;
            }
            while (!stop) {
                try {
                    process();
                } catch (Throwable e) {
                    sendError(e);
                }
            }
        } catch (Throwable e) {
        } finally {
            close();
        }
    }

    private Session createSession(String dbName, String originalURL, String userName, Transfer transfer)
            throws IOException {
        byte[] userPasswordHash = transfer.readBytes();
        byte[] filePasswordHash = transfer.readBytes();
        byte[] fileEncryptionKey = transfer.readBytes();

        ConnectionInfo ci = new ConnectionInfo(originalURL, dbName);

        Properties originalProperties = new Properties();

        String key, value;
        int len = transfer.readInt();
        for (int i = 0; i < len; i++) {
            key = transfer.readString();
            value = transfer.readString();
            ci.addProperty(key, value, true); // 一些不严谨的client driver可能会发送重复的属性名
            originalProperties.setProperty(key, value);
        }
        String baseDir = SysProperties.getBaseDirSilently();

        // override client's requested properties with server settings
        if (baseDir != null) {
            ci.setBaseDir(baseDir);
        }
        ci.setUserName(userName);
        ci.setUserPasswordHash(userPasswordHash);
        ci.setFilePasswordHash(filePasswordHash);
        ci.setFileEncryptionKey(fileEncryptionKey);

        try {
            Session session = ci.getSessionFactory().createSession(ci);
            return session;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private void closeSession() {
        if (session != null) {
            RuntimeException closeError = null;
            try {
                session.prepareStatement("ROLLBACK", -1).executeUpdate();
            } catch (RuntimeException e) {
                closeError = e;
            } catch (Exception e) {
            }
            try {
                session.close();
            } catch (RuntimeException e) {
                if (closeError == null) {
                    closeError = e;
                }
            } catch (Exception e) {
            } finally {
                session = null;
            }
            if (closeError != null) {
                throw closeError;
            }
        }
    }

    /**
     * Close a connection.
     */
    @Override
    public void close() {
        try {
            stop = true;
            closeSession();
        } finally {
            transfer.close();
            group.remove(this);
        }
    }

    private void sendError(Throwable t) {
        try {
            SQLException e = DbException.convert(t).getSQLException();
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            String trace = writer.toString();
            String message;
            String sql;
            if (e instanceof JdbcSQLException) {
                JdbcSQLException j = (JdbcSQLException) e;
                message = j.getOriginalMessage();
                sql = j.getSQL();
            } else {
                message = e.getMessage();
                sql = null;
            }

            transfer.reset(); // 为什么要reset? 见reset中的注释

            transfer.writeInt(Session.STATUS_ERROR).writeString(e.getSQLState()).writeString(message).writeString(sql)
                    .writeInt(e.getErrorCode()).writeString(trace).flush();
        } catch (Exception e2) {
            // if writing the error does not work, close the connection
            stop = true;
        }
    }

    private void setParameters(PreparedStatement command) throws IOException {
        int len = transfer.readInt();
        ArrayList<? extends CommandParameter> params = command.getParameters();
        for (int i = 0; i < len; i++) {
            CommandParameter p = params.get(i);
            p.setValue(transfer.readValue());
        }
    }

    private void process() throws IOException {
        int operation = transfer.readInt();
        switch (operation) {
        case Session.SESSION_PREPARE_READ_PARAMS:
        case Session.SESSION_PREPARE: {
            int id = transfer.readInt();
            String sql = transfer.readString();
            int old = session.getModificationId();
            PreparedStatement command = session.prepareStatement(sql, -1);
            boolean readonly = command.isReadOnly();
            cache.addObject(id, command);
            boolean isQuery = command.isQuery();
            ArrayList<? extends CommandParameter> params = command.getParameters();
            transfer.writeInt(getState(old)).writeBoolean(isQuery).writeBoolean(readonly).writeInt(params.size());
            if (operation == Session.SESSION_PREPARE_READ_PARAMS) {
                for (CommandParameter p : params) {
                    writeMetaData(transfer, p);
                }
            }
            transfer.flush();
            break;
        }
        case Session.SESSION_CLOSE: {
            stop = true;
            closeSession();
            transfer.writeInt(Session.STATUS_OK).flush();
            close();
            break;
        }
        case Session.COMMAND_GET_META_DATA: {
            int id = transfer.readInt();
            int objectId = transfer.readInt();
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            Result result = command.getMetaData();
            cache.addObject(objectId, result);
            int columnCount = result.getVisibleColumnCount();
            transfer.writeInt(Session.STATUS_OK).writeInt(columnCount).writeInt(0);
            for (int i = 0; i < columnCount; i++) {
                writeColumn(transfer, result, i);
            }
            transfer.flush();
            break;
        }
        case Session.COMMAND_EXECUTE_QUERY: {
            int id = transfer.readInt();
            int objectId = transfer.readInt();
            int maxRows = transfer.readInt();
            int fetchSize = transfer.readInt();
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            command.setFetchSize(fetchSize);
            setParameters(command);
            int old = session.getModificationId();
            Result result;
            synchronized (session) {
                result = command.executeQuery(maxRows, false);
            }
            cache.addObject(objectId, result);
            int columnCount = result.getVisibleColumnCount();
            int state = getState(old);
            transfer.writeInt(state);

            if (operation == Session.COMMAND_EXECUTE_DISTRIBUTED_QUERY)
                transfer.writeString(session.getTransaction().getLocalTransactionNames());

            transfer.writeInt(columnCount);
            int rowCount = result.getRowCount();
            transfer.writeInt(rowCount);
            for (int i = 0; i < columnCount; i++) {
                writeColumn(transfer, result, i);
            }
            int fetch = fetchSize;
            if (rowCount != -1)
                fetch = Math.min(rowCount, fetchSize);
            sendRow(result, fetch);
            transfer.flush();
            break;
        }
        case Session.COMMAND_EXECUTE_UPDATE: {
            int id = transfer.readInt();
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            setParameters(command);
            int old = session.getModificationId();
            int updateCount;
            synchronized (session) {
                updateCount = command.executeUpdate();
            }
            int status;
            if (session.isClosed()) {
                status = Session.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            if (operation == Session.COMMAND_EXECUTE_DISTRIBUTED_UPDATE)
                transfer.writeString(session.getTransaction().getLocalTransactionNames());

            transfer.writeInt(updateCount).writeBoolean(session.isAutoCommit());
            transfer.flush();
            break;
        }
        case Session.COMMAND_CLOSE: {
            int id = transfer.readInt();
            PreparedStatement command = (PreparedStatement) cache.getObject(id, true);
            if (command != null) {
                command.close();
                cache.freeObject(id);
            }
            break;
        }
        case Session.RESULT_FETCH_ROWS: {
            int id = transfer.readInt();
            int count = transfer.readInt();
            Result result = (Result) cache.getObject(id, false);
            transfer.writeInt(Session.STATUS_OK);
            sendRow(result, count);
            transfer.flush();
            break;
        }
        case Session.RESULT_RESET: {
            int id = transfer.readInt();
            Result result = (Result) cache.getObject(id, false);
            result.reset();
            break;
        }
        case Session.RESULT_CLOSE: {
            int id = transfer.readInt();
            Result result = (Result) cache.getObject(id, true);
            if (result != null) {
                result.close();
                cache.freeObject(id);
            }
            break;
        }
        case Session.CHANGE_ID: {
            int oldId = transfer.readInt();
            int newId = transfer.readInt();
            Object obj = cache.getObject(oldId, false);
            cache.freeObject(oldId);
            cache.addObject(newId, obj);
            break;
        }
        default:
            closeSession();
            close();
        }
    }

    private int getState(int oldModificationId) {
        if (session.getModificationId() == oldModificationId) {
            return Session.STATUS_OK;
        }
        return Session.STATUS_OK_STATE_CHANGED;
    }

    private void sendRow(Result result, int count) throws IOException {
        try {
            int visibleColumnCount = result.getVisibleColumnCount();
            for (int i = 0; i < count; i++) {
                if (result.next()) {
                    transfer.writeBoolean(true);
                    Value[] v = result.currentRow();
                    for (int j = 0; j < visibleColumnCount; j++) {
                        transfer.writeValue(v[j]);
                    }
                } else {
                    transfer.writeBoolean(false);
                    break;
                }
            }
        } catch (Throwable e) {
            // 如果取结果集的下一行记录时发生了异常，
            // 比如在HBase环境一个结果集可能涉及多个region，当切换到下一个region时此region有可能在进行split，
            // 此时就会抛异常，所以结果集包必须加一个结束标记，结果集包后面跟一个异常包。
            transfer.writeBoolean(false);
            throw DbException.convert(e);
        }
    }

    void setThread(Thread thread) {
        this.thread = thread;
    }

    Thread getThread() {
        return thread;
    }

    /**
     * Cancel a running statement.
     *
     * @param targetSessionId the session id
     * @param statementId the statement to cancel
     */
    void cancelStatement(String targetSessionId, int statementId) {
        if (StringUtils.equals(targetSessionId, this.sessionId)) {
            PreparedStatement cmd = (PreparedStatement) cache.getObject(statementId, false);
            cmd.cancel();
        }
    }

    /**
     * An input stream with a position.
     */
    static class CachedInputStream extends FilterInputStream {

        private static final ByteArrayInputStream DUMMY = new ByteArrayInputStream(new byte[0]);
        private long pos;

        CachedInputStream(InputStream in) {
            super(in == null ? DUMMY : in);
            if (in == null) {
                pos = -1;
            }
        }

        @Override
        public int read(byte[] buff, int off, int len) throws IOException {
            len = super.read(buff, off, len);
            if (len > 0) {
                pos += len;
            }
            return len;
        }

        @Override
        public int read() throws IOException {
            int x = in.read();
            if (x >= 0) {
                pos++;
            }
            return x;
        }

        @Override
        public long skip(long n) throws IOException {
            n = super.skip(n);
            if (n > 0) {
                pos += n;
            }
            return n;
        }

        public long getPos() {
            return pos;
        }

    }

    /**
     * Write the parameter meta data to the transfer object.
     *
     * @param transfer the transfer object
     * @param p the parameter
     */
    private static void writeMetaData(Transfer transfer, CommandParameter p) throws IOException {
        transfer.writeInt(p.getType());
        transfer.writeLong(p.getPrecision());
        transfer.writeInt(p.getScale());
        transfer.writeInt(p.getNullable());
    }

    /**
     * Write a result column to the given output.
     *
     * @param out the object to where to write the data
     * @param result the result
     * @param i the column index
     */
    public static void writeColumn(Transfer out, Result result, int i) throws IOException {
        out.writeString(result.getAlias(i));
        out.writeString(result.getSchemaName(i));
        out.writeString(result.getTableName(i));
        out.writeString(result.getColumnName(i));
        out.writeInt(result.getColumnType(i));
        out.writeLong(result.getColumnPrecision(i));
        out.writeInt(result.getColumnScale(i));
        out.writeInt(result.getDisplaySize(i));
        out.writeBoolean(result.isAutoIncrement(i));
        out.writeInt(result.getNullable(i));
    }

}

/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.server;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import org.lealone.api.ErrorCode;
import org.lealone.api.ParameterInterface;
import org.lealone.command.BackendBatchCommand;
import org.lealone.command.Command;
import org.lealone.engine.ConnectionInfo;
import org.lealone.engine.Constants;
import org.lealone.engine.FrontendSession;
import org.lealone.engine.LobStorageInterface;
import org.lealone.engine.Session;
import org.lealone.engine.SysProperties;
import org.lealone.expression.Parameter;
import org.lealone.message.DbException;
import org.lealone.message.JdbcSQLException;
import org.lealone.result.ResultColumn;
import org.lealone.result.ResultInterface;
import org.lealone.transaction.TransactionStatusTable;
import org.lealone.util.IOUtils;
import org.lealone.util.New;
import org.lealone.util.SmallLRUCache;
import org.lealone.util.SmallMap;
import org.lealone.util.StringUtils;
import org.lealone.value.Transfer;
import org.lealone.value.Value;
import org.lealone.value.ValueLobDb;

/**
 * One server thread is opened per client connection.
 */
public class TcpServerThread implements Runnable {

    private final SmallMap cache = new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private final SmallLRUCache<Long, CachedInputStream> lobs = SmallLRUCache.newInstance(Math.max(
            SysProperties.SERVER_CACHED_OBJECTS, SysProperties.SERVER_RESULT_SET_FETCH_SIZE * 5));

    private final TcpServer server;
    private final int threadId;
    private final Transfer transfer;

    private Session session;
    private boolean stop;
    private Thread thread;
    //private Command commit;
    private int clientVersion;
    private String sessionId;

    protected TcpServerThread(Socket socket, TcpServer server, int threadId) {
        this.server = server;
        this.threadId = threadId;
        transfer = new Transfer(null, socket);
    }

    private void trace(String s) {
        server.trace(this + " " + s);
    }

    @Override
    public void run() {
        try {
            transfer.init();
            if (server.isTraceEnabled())
                trace("Connect");
            // TODO server: should support a list of allowed databases
            // and a list of allowed clients
            try {
                if (!server.allow(transfer.getSocket())) {
                    throw DbException.get(ErrorCode.REMOTE_CONNECTION_NOT_ALLOWED);
                }
                int minClientVersion = transfer.readInt();
                if (minClientVersion < Constants.TCP_PROTOCOL_VERSION_1) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + clientVersion, ""
                            + Constants.TCP_PROTOCOL_VERSION_1);
                } else if (minClientVersion > Constants.TCP_PROTOCOL_VERSION_1) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + clientVersion, ""
                            + Constants.TCP_PROTOCOL_VERSION_1);
                }
                int maxClientVersion = transfer.readInt();
                if (maxClientVersion >= Constants.TCP_PROTOCOL_VERSION_1) {
                    clientVersion = Constants.TCP_PROTOCOL_VERSION_1;
                } else {
                    clientVersion = minClientVersion;
                }
                transfer.setVersion(clientVersion);
                String db = transfer.readString();
                String originalURL = transfer.readString();
                if (db == null && originalURL == null) {
                    String targetSessionId = transfer.readString();
                    int command = transfer.readInt();
                    stop = true;
                    if (command == FrontendSession.SESSION_CANCEL_STATEMENT) {
                        // cancel a running statement
                        int statementId = transfer.readInt();
                        server.cancelStatement(targetSessionId, statementId);
                    } else if (command == FrontendSession.SESSION_CHECK_KEY) {
                        // check if this is the correct server
                        db = server.checkKeyAndGetDatabaseName(targetSessionId);
                        if (!targetSessionId.equals(db)) {
                            transfer.writeInt(FrontendSession.STATUS_OK);
                        } else {
                            transfer.writeInt(FrontendSession.STATUS_ERROR);
                        }
                    }
                }

                String userName = transfer.readString();
                userName = StringUtils.toUpperEnglish(userName);
                session = createSession(db, originalURL, userName, transfer);
                transfer.setSession(session);
                transfer.writeInt(FrontendSession.STATUS_OK);
                transfer.writeInt(clientVersion);
                transfer.flush();
                server.addConnection(threadId, originalURL, userName);
                if (server.isTraceEnabled())
                    trace("Connected");
            } catch (Throwable e) {
                sendError(e);
                stop = true;
            }
            while (!stop) {
                try {
                    process();
                } catch (Throwable e) {
                    if (server.isTraceEnabled())
                        server.traceError(e);
                    sendError(e);
                }
            }
            if (server.isTraceEnabled())
                trace("Disconnect");
        } catch (Throwable e) {
            server.traceError(e);
        } finally {
            close();
        }
    }

    protected ConnectionInfo createConnectionInfo(String originalURL, String dbName) {
        return new ConnectionInfo(originalURL, dbName);
    }

    private Session createSession(String dbName, String originalURL, String userName, Transfer transfer) throws IOException {
        byte[] userPasswordHash = transfer.readBytes();
        byte[] filePasswordHash = transfer.readBytes();
        byte[] fileEncryptionKey = transfer.readBytes();

        Properties originalProperties = new Properties();

        int len = transfer.readInt();
        for (int i = 0; i < len; i++) {
            originalProperties.setProperty(transfer.readString(), transfer.readString());
        }
        String baseDir = server.getBaseDir();
        if (baseDir == null) {
            baseDir = SysProperties.getBaseDir();
        }

        dbName = server.checkKeyAndGetDatabaseName(dbName);
        ConnectionInfo ci = createConnectionInfo(originalURL, dbName);

        // override client's requested properties with server settings
        if (baseDir != null) {
            ci.setBaseDir(baseDir);
        }
        if (server.getIfExists()) {
            ci.setProperty("IFEXISTS", "TRUE");
        }
        ci.setUserName(userName);

        ci.setUserPasswordHash(userPasswordHash);
        ci.setFilePasswordHash(filePasswordHash);
        ci.setFileEncryptionKey(fileEncryptionKey);
        ci.readProperties(originalProperties);

        originalProperties.setProperty("user", userName);
        if (userPasswordHash != null)
            originalProperties.put("_userPasswordHash_", userPasswordHash);
        if (filePasswordHash != null)
            originalProperties.put("_filePasswordHash_", filePasswordHash);

        try {
            //先删除"IS_LOCAL"，否则在createSession过程中会执行 SET IS_LOCAL命令导致错误
            boolean isLocal = ci.removeProperty("IS_LOCAL", false);
            Session session = (Session) ci.getSessionFactory().createSession(ci);
            session.setOriginalProperties(originalProperties);
            session.setLocal(isLocal);
            session.setHostAndPort(server.getListenAddress(), server.getPort());
            return session;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private void closeSession() {
        if (session != null) {
            RuntimeException closeError = null;
            try {
                Command rollback = session.prepareLocal("ROLLBACK");
                rollback.executeUpdate();
            } catch (RuntimeException e) {
                closeError = e;
                server.traceError(e);
            } catch (Exception e) {
                server.traceError(e);
            }
            try {
                session.close();
                server.removeConnection(threadId);
            } catch (RuntimeException e) {
                if (closeError == null) {
                    closeError = e;
                    server.traceError(e);
                }
            } catch (Exception e) {
                server.traceError(e);
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
    void close() {
        try {
            stop = true;
            closeSession();
        } catch (Exception e) {
            server.traceError(e);
        } finally {
            transfer.close();
            if (server.isTraceEnabled())
                trace("Close");
            server.remove(this);
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
            transfer.writeInt(FrontendSession.STATUS_ERROR).writeString(e.getSQLState()).writeString(message).writeString(sql)
                    .writeInt(e.getErrorCode()).writeString(trace).flush();
        } catch (Exception e2) {
            if (!transfer.isClosed()) {
                server.traceError(e2);
            }
            // if writing the error does not work, close the connection
            stop = true;
        }
    }

    private void setParameters(Command command) throws IOException {
        int len = transfer.readInt();
        ArrayList<? extends ParameterInterface> params = command.getParameters();
        for (int i = 0; i < len; i++) {
            Parameter p = (Parameter) params.get(i);
            p.setValue(transfer.readValue());
        }
    }

    private void executeBatch(int size, BackendBatchCommand command) throws IOException {
        int old = session.getModificationId();
        synchronized (session) {
            command.executeUpdate();
        }

        int status;
        if (session.isClosed()) {
            status = FrontendSession.STATUS_CLOSED;
        } else {
            status = getState(old);
        }
        transfer.writeInt(status);
        int[] result = command.getResult();
        command.close();
        for (int i = 0; i < size; i++)
            transfer.writeInt(result[i]);
        transfer.flush();
    }

    private void process() throws IOException {
        int operation = transfer.readInt();
        switch (operation) {
        case FrontendSession.SESSION_PREPARE_READ_PARAMS:
        case FrontendSession.SESSION_PREPARE: {
            int id = transfer.readInt();
            String sql = transfer.readString();
            int old = session.getModificationId();
            Command command = session.prepareCommand(sql);
            boolean readonly = command.isReadOnly();
            cache.addObject(id, command);
            boolean isQuery = command.isQuery();
            ArrayList<? extends ParameterInterface> params = command.getParameters();
            transfer.writeInt(getState(old)).writeBoolean(isQuery).writeBoolean(readonly).writeInt(params.size());
            if (operation == FrontendSession.SESSION_PREPARE_READ_PARAMS) {
                for (ParameterInterface p : params) {
                    writeMetaData(transfer, p);
                }
            }
            transfer.flush();
            break;
        }
        case FrontendSession.SESSION_CLOSE: {
            stop = true;
            closeSession();
            transfer.writeInt(FrontendSession.STATUS_OK).flush();
            close();
            break;
        }
        //        case FrontendSession.COMMAND_COMMIT: {
        //            if (commit == null) {
        //                commit = session.prepareLocal("COMMIT");
        //            }
        //            int old = session.getModificationId();
        //            commit.executeUpdate();
        //            transfer.writeInt(getState(old)).flush();
        //            break;
        //        }
        case FrontendSession.COMMAND_GET_META_DATA: {
            int id = transfer.readInt();
            int objectId = transfer.readInt();
            Command command = (Command) cache.getObject(id, false);
            ResultInterface result = command.getMetaData();
            cache.addObject(objectId, result);
            int columnCount = result.getVisibleColumnCount();
            transfer.writeInt(FrontendSession.STATUS_OK).writeInt(columnCount).writeInt(0);
            for (int i = 0; i < columnCount; i++) {
                ResultColumn.writeColumn(transfer, result, i);
            }
            transfer.flush();
            break;
        }
        case FrontendSession.COMMAND_EXECUTE_DISTRIBUTED_QUERY: {
            session.setAutoCommit(false);
            session.setRoot(false);
        }
        case FrontendSession.COMMAND_EXECUTE_QUERY: {
            int id = transfer.readInt();
            int objectId = transfer.readInt();
            int maxRows = transfer.readInt();
            int fetchSize = transfer.readInt();
            Command command = (Command) cache.getObject(id, false);
            command.getPrepared().setFetchSize(fetchSize);
            setParameters(command);
            int old = session.getModificationId();
            ResultInterface result;
            synchronized (session) {
                result = command.executeQuery(maxRows, false);
            }
            cache.addObject(objectId, result);
            int columnCount = result.getVisibleColumnCount();
            int state = getState(old);
            transfer.writeInt(state);

            if (operation == FrontendSession.COMMAND_EXECUTE_DISTRIBUTED_QUERY)
                transfer.writeString(session.getTransaction().getLocalTransactionNames());

            transfer.writeInt(columnCount);
            int rowCount = result.getRowCount();
            transfer.writeInt(rowCount);
            for (int i = 0; i < columnCount; i++) {
                ResultColumn.writeColumn(transfer, result, i);
            }
            int fetch = fetchSize;
            if (rowCount != -1)
                fetch = Math.min(rowCount, fetchSize);
            sendRow(result, fetch);
            transfer.flush();
            break;
        }
        case FrontendSession.COMMAND_EXECUTE_DISTRIBUTED_UPDATE: {
            session.setAutoCommit(false);
            session.setRoot(false);
        }
        case FrontendSession.COMMAND_EXECUTE_UPDATE: {
            int id = transfer.readInt();
            Command command = (Command) cache.getObject(id, false);
            setParameters(command);
            int old = session.getModificationId();
            int updateCount;
            synchronized (session) {
                updateCount = command.executeUpdate();
            }
            int status;
            if (session.isClosed()) {
                status = FrontendSession.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            if (operation == FrontendSession.COMMAND_EXECUTE_DISTRIBUTED_UPDATE)
                transfer.writeString(session.getTransaction().getLocalTransactionNames());

            transfer.writeInt(updateCount).writeBoolean(session.isAutoCommit());
            transfer.flush();
            break;
        }
        case FrontendSession.COMMAND_EXECUTE_DISTRIBUTED_COMMIT: {
            int old = session.getModificationId();
            synchronized (session) {
                session.commit(false, transfer.readString());
            }
            int status;
            if (session.isClosed()) {
                status = FrontendSession.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            transfer.flush();
            break;
        }
        case FrontendSession.COMMAND_EXECUTE_DISTRIBUTED_ROLLBACK: {
            int old = session.getModificationId();
            synchronized (session) {
                session.rollback();
            }
            int status;
            if (session.isClosed()) {
                status = FrontendSession.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            transfer.flush();
            break;
        }
        case FrontendSession.COMMAND_EXECUTE_DISTRIBUTED_SAVEPOINT_ADD:
        case FrontendSession.COMMAND_EXECUTE_DISTRIBUTED_SAVEPOINT_ROLLBACK: {
            int old = session.getModificationId();
            String name = transfer.readString();
            synchronized (session) {
                if (operation == FrontendSession.COMMAND_EXECUTE_DISTRIBUTED_SAVEPOINT_ADD)
                    session.addSavepoint(name);
                else
                    session.rollbackToSavepoint(name);
            }
            int status;
            if (session.isClosed()) {
                status = FrontendSession.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            transfer.flush();
            break;
        }
        case FrontendSession.COMMAND_EXECUTE_TRANSACTION_VALIDATE: {
            int old = session.getModificationId();
            boolean isValid = TransactionStatusTable.isValid(transfer.readString());
            int status;
            if (session.isClosed()) {
                status = FrontendSession.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            transfer.writeBoolean(isValid);
            transfer.flush();
            break;
        }
        case FrontendSession.COMMAND_EXECUTE_BATCH_UPDATE_STATEMENT: {
            int size = transfer.readInt();
            ArrayList<String> batchCommands = New.arrayList(size);
            for (int i = 0; i < size; i++)
                batchCommands.add(transfer.readString());

            BackendBatchCommand command = new BackendBatchCommand(session, batchCommands);
            executeBatch(size, command);
            break;
        }
        case FrontendSession.COMMAND_EXECUTE_BATCH_UPDATE_PREPAREDSTATEMENT: {
            int id = transfer.readInt();
            int size = transfer.readInt();
            Command preparedCommand = (Command) cache.getObject(id, false);
            ArrayList<Value[]> batchParameters = New.arrayList(size);
            int paramsSize = preparedCommand.getParameters().size();
            Value[] values;
            for (int i = 0; i < size; i++) {
                values = new Value[paramsSize];
                for (int j = 0; j < paramsSize; j++) {
                    values[j] = transfer.readValue();
                }
                batchParameters.add(values);
            }
            BackendBatchCommand command = new BackendBatchCommand(session, preparedCommand, batchParameters);
            executeBatch(size, command);
            break;
        }
        case FrontendSession.COMMAND_CLOSE: {
            int id = transfer.readInt();
            Command command = (Command) cache.getObject(id, true);
            if (command != null) {
                command.close();
                cache.freeObject(id);
            }
            break;
        }
        case FrontendSession.RESULT_FETCH_ROWS: {
            int id = transfer.readInt();
            int count = transfer.readInt();
            ResultInterface result = (ResultInterface) cache.getObject(id, false);
            transfer.writeInt(FrontendSession.STATUS_OK);
            sendRow(result, count);
            transfer.flush();
            break;
        }
        case FrontendSession.RESULT_RESET: {
            int id = transfer.readInt();
            ResultInterface result = (ResultInterface) cache.getObject(id, false);
            result.reset();
            break;
        }
        case FrontendSession.RESULT_CLOSE: {
            int id = transfer.readInt();
            ResultInterface result = (ResultInterface) cache.getObject(id, true);
            if (result != null) {
                result.close();
                cache.freeObject(id);
            }
            break;
        }
        case FrontendSession.CHANGE_ID: {
            int oldId = transfer.readInt();
            int newId = transfer.readInt();
            Object obj = cache.getObject(oldId, false);
            cache.freeObject(oldId);
            cache.addObject(newId, obj);
            break;
        }
        case FrontendSession.SESSION_SET_ID: {
            sessionId = transfer.readString();
            transfer.writeInt(FrontendSession.STATUS_OK).flush();
            break;
        }
        case FrontendSession.SESSION_SET_AUTOCOMMIT: {
            boolean autoCommit = transfer.readBoolean();
            session.setAutoCommit(autoCommit);
            transfer.writeInt(FrontendSession.STATUS_OK).flush();
            break;
        }
        case FrontendSession.SESSION_UNDO_LOG_POS: {
            transfer.writeInt(FrontendSession.STATUS_OK).writeInt(session.getUndoLogPos()).flush();
            break;
        }
        case FrontendSession.LOB_READ: {
            long lobId = transfer.readLong();
            byte[] hmac = transfer.readBytes();
            CachedInputStream in = lobs.get(lobId);
            if (in == null) {
                in = new CachedInputStream(null);
                lobs.put(lobId, in);
            }
            long offset = transfer.readLong();
            int length = transfer.readInt();
            transfer.verifyLobMac(hmac, lobId);
            if (in.getPos() != offset) {
                LobStorageInterface lobStorage = session.getDataHandler().getLobStorage();
                // only the lob id is used
                ValueLobDb lob = ValueLobDb.create(Value.BLOB, null, -1, lobId, hmac, -1);
                InputStream lobIn = lobStorage.getInputStream(lob, hmac, -1);
                in = new CachedInputStream(lobIn);
                lobs.put(lobId, in);
                lobIn.skip(offset);
            }
            // limit the buffer size
            length = Math.min(16 * Constants.IO_BUFFER_SIZE, length);
            byte[] buff = new byte[length];
            length = IOUtils.readFully(in, buff, length);
            transfer.writeInt(FrontendSession.STATUS_OK);
            transfer.writeInt(length);
            transfer.writeBytes(buff, 0, length);
            transfer.flush();
            break;
        }
        default:
            if (server.isTraceEnabled())
                trace("Unknown operation: " + operation);
            closeSession();
            close();
        }
    }

    private int getState(int oldModificationId) {
        if (session.getModificationId() == oldModificationId) {
            return FrontendSession.STATUS_OK;
        }
        return FrontendSession.STATUS_OK_STATE_CHANGED;
    }

    private void sendRow(ResultInterface result, int count) throws IOException {
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
            //如果取结果集的下一行记录时发生了异常，
            //比如在HBase环境一个结果集可能涉及多个region，当切换到下一个region时此region有可能在进行split，
            //此时就会抛异常，所以结果集包必须加一个结束标记，结果集包后面跟一个异常包。
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
            Command cmd = (Command) cache.getObject(statementId, false);
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
    private static void writeMetaData(Transfer transfer, ParameterInterface p) throws IOException {
        transfer.writeInt(p.getType());
        transfer.writeLong(p.getPrecision());
        transfer.writeInt(p.getScale());
        transfer.writeInt(p.getNullable());
    }

}

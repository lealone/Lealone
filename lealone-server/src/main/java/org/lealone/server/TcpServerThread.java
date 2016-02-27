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
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.exceptions.JdbcSQLException;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.New;
import org.lealone.common.util.SmallLRUCache;
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
import org.lealone.db.value.ValueLob;
import org.lealone.replication.Replication;
import org.lealone.sql.BatchStatement;
import org.lealone.sql.PreparedStatement;
import org.lealone.storage.LobStorage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.WriteBuffer;
import org.lealone.storage.type.WriteBufferPool;

/**
 * One server thread is opened per client connection.
 * 
 * @author H2 Group
 * @author zhh
 */
public class TcpServerThread extends Thread implements Comparable<TcpServerThread> {

    private final SmallMap cache = new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private SmallLRUCache<Long, CachedInputStream> lobs; // 大多数情况下都不使用lob，所以延迟初始化

    private final TcpServer server;
    private final Transfer transfer;

    private Session session;
    private String sessionId;
    private boolean stop;

    protected TcpServerThread(Socket socket, TcpServer server) {
        this.server = server;
        this.transfer = new Transfer(socket);
    }

    private void trace(String s) {
        server.trace(this + " " + s);
    }

    @Override
    public int compareTo(TcpServerThread o) {
        return getName().compareTo(o.getName());
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
                if (minClientVersion < Constants.TCP_PROTOCOL_VERSION_MIN) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + minClientVersion, ""
                            + Constants.TCP_PROTOCOL_VERSION_MIN);
                } else if (minClientVersion > Constants.TCP_PROTOCOL_VERSION_MAX) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + minClientVersion, ""
                            + Constants.TCP_PROTOCOL_VERSION_MAX);
                }
                int clientVersion;
                int maxClientVersion = transfer.readInt();
                if (maxClientVersion >= Constants.TCP_PROTOCOL_VERSION_MAX) {
                    clientVersion = Constants.TCP_PROTOCOL_VERSION_CURRENT;
                } else {
                    clientVersion = minClientVersion;
                }
                transfer.setVersion(clientVersion);
                String dbName = transfer.readString();
                String originalURL = transfer.readString();
                if (dbName == null && originalURL == null) {
                    String targetSessionId = transfer.readString();
                    int command = transfer.readInt();
                    stop = true;
                    if (command == Session.SESSION_CANCEL_STATEMENT) {
                        // cancel a running statement
                        int statementId = transfer.readInt();
                        server.cancelStatement(targetSessionId, statementId);
                    } else {
                        throw DbException.throwInternalError();
                    }
                }

                String userName = transfer.readString();
                userName = StringUtils.toUpperEnglish(userName);
                session = createSession(originalURL, dbName, userName);
                transfer.setSession(session);
                transfer.writeInt(Session.STATUS_OK);
                transfer.writeInt(clientVersion);
                transfer.flush();
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

    private Session createSession(String originalURL, String dbName, String userName) throws IOException {
        ConnectionInfo ci = new ConnectionInfo(originalURL, dbName);
        ci.setUserName(userName);
        ci.setUserPasswordHash(transfer.readBytes());
        ci.setFilePasswordHash(transfer.readBytes());
        ci.setFileEncryptionKey(transfer.readBytes());

        int len = transfer.readInt();
        for (int i = 0; i < len; i++) {
            String key = transfer.readString();
            String value = transfer.readString();
            ci.addProperty(key, value, true); // 一些不严谨的client driver可能会发送重复的属性名
        }

        String baseDir = server.getBaseDir();
        if (baseDir == null) {
            baseDir = SysProperties.getBaseDirSilently();
        }

        // override client's requested properties with server settings
        if (baseDir != null) {
            ci.setBaseDir(baseDir);
        }
        if (server.getIfExists()) {
            ci.setProperty("IFEXISTS", "TRUE");
        }

        try {
            Session session = ci.getSessionFactory().createSession(ci);
            if (ci.getProperty("IS_LOCAL") != null)
                session.setLocal(Boolean.parseBoolean(ci.getProperty("IS_LOCAL")));
            return session;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private void closeSession() {
        if (session != null) {
            RuntimeException closeError = null;
            try {
                session.prepareStatement("ROLLBACK", -1).update();
            } catch (RuntimeException e) {
                closeError = e;
                server.traceError(e);
            } catch (Exception e) {
                server.traceError(e);
            }
            try {
                session.close();
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
            if (!transfer.isClosed()) {
                server.traceError(e2);
            }
            // if writing the error does not work, close the connection
            stop = true;
        }
    }

    private void setParameters(PreparedStatement command) throws IOException {
        int len = transfer.readInt();
        List<? extends CommandParameter> params = command.getParameters();
        for (int i = 0; i < len; i++) {
            CommandParameter p = params.get(i);
            p.setValue(transfer.readValue());
        }
    }

    /**
     * Write the parameter meta data to the transfer object.
     *
     * @param p the parameter
     */
    private void writeParameterMetaData(CommandParameter p) throws IOException {
        transfer.writeInt(p.getType());
        transfer.writeLong(p.getPrecision());
        transfer.writeInt(p.getScale());
        transfer.writeInt(p.getNullable());
    }

    /**
     * Write a result column to the given output.
     *
     * @param result the result
     * @param i the column index
     */
    private void writeColumn(Result result, int i) throws IOException {
        transfer.writeString(result.getAlias(i));
        transfer.writeString(result.getSchemaName(i));
        transfer.writeString(result.getTableName(i));
        transfer.writeString(result.getColumnName(i));
        transfer.writeInt(result.getColumnType(i));
        transfer.writeLong(result.getColumnPrecision(i));
        transfer.writeInt(result.getColumnScale(i));
        transfer.writeInt(result.getDisplaySize(i));
        transfer.writeBoolean(result.isAutoIncrement(i));
        transfer.writeInt(result.getNullable(i));
    }

    private void writeRow(Result result, int count) throws IOException {
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
            // 结果集包必须加一个结束标记，结果集包后面跟一个异常包。
            transfer.writeBoolean(false);
            throw DbException.convert(e);
        }
    }

    private int getState(int oldModificationId) {
        if (session.getModificationId() == oldModificationId) {
            return Session.STATUS_OK;
        }
        return Session.STATUS_OK_STATE_CHANGED;
    }

    private void executeBatch(int size, BatchStatement command) throws IOException {
        int old = session.getModificationId();
        synchronized (session) {
            command.update();
        }

        int status;
        if (session.isClosed()) {
            status = Session.STATUS_CLOSED;
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

    private void executeQuery(PreparedStatement command, int operation, int objectId, int maxRows, int fetchSize,
            int oldModificationId) throws IOException {
        Result result;
        synchronized (session) {
            result = command.query(maxRows, false);
        }
        cache.addObject(objectId, result);
        transfer.writeInt(getState(oldModificationId));

        if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY
                || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY)
            transfer.writeString(session.getTransaction().getLocalTransactionNames());

        int columnCount = result.getVisibleColumnCount();
        transfer.writeInt(columnCount);
        int rowCount = result.getRowCount();
        transfer.writeInt(rowCount);
        for (int i = 0; i < columnCount; i++) {
            writeColumn(result, i);
        }
        int fetch = fetchSize;
        if (rowCount != -1)
            fetch = Math.min(rowCount, fetchSize);
        writeRow(result, fetch);
        transfer.flush();
    }

    private void executeUpdate(PreparedStatement command, int operation, int oldModificationId) throws IOException {
        int updateCount;
        synchronized (session) {
            updateCount = command.update();
        }
        int status;
        if (session.isClosed()) {
            status = Session.STATUS_CLOSED;
        } else {
            status = getState(oldModificationId);
        }
        transfer.writeInt(status);
        if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE
                || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE)
            transfer.writeString(session.getTransaction().getLocalTransactionNames());

        transfer.writeInt(updateCount);
        transfer.flush();
    }

    private void process() throws IOException {
        int operation = transfer.readInt();
        switch (operation) {
        case Session.COMMAND_PREPARE_READ_PARAMS:
        case Session.COMMAND_PREPARE: {
            int id = transfer.readInt();
            String sql = transfer.readString();
            int old = session.getModificationId();
            PreparedStatement command = session.prepareStatement(sql, -1);
            cache.addObject(id, command);
            boolean isQuery = command.isQuery();
            transfer.writeInt(getState(old)).writeBoolean(isQuery);
            if (operation == Session.COMMAND_PREPARE_READ_PARAMS) {
                List<? extends CommandParameter> params = command.getParameters();
                transfer.writeInt(params.size());
                for (CommandParameter p : params) {
                    writeParameterMetaData(p);
                }
            }
            transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY: {
            session.setAutoCommit(false);
            session.setRoot(false);
        }
        case Session.COMMAND_QUERY: {
            int id = transfer.readInt();
            String sql = transfer.readString();
            int objectId = transfer.readInt();
            int maxRows = transfer.readInt();
            int fetchSize = transfer.readInt();
            int old = session.getModificationId();
            PreparedStatement command = session.prepareStatement(sql, fetchSize);
            cache.addObject(id, command);
            executeQuery(command, operation, objectId, maxRows, fetchSize, old);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY: {
            session.setAutoCommit(false);
            session.setRoot(false);
        }
        case Session.COMMAND_PREPARED_QUERY: {
            int id = transfer.readInt();
            int objectId = transfer.readInt();
            int maxRows = transfer.readInt();
            int fetchSize = transfer.readInt();
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            command.setFetchSize(fetchSize);
            setParameters(command);
            int old = session.getModificationId();
            executeQuery(command, operation, objectId, maxRows, fetchSize, old);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE: {
            session.setAutoCommit(false);
            session.setRoot(false);
        }
        case Session.COMMAND_UPDATE:
        case Session.COMMAND_REPLICATION_UPDATE: {
            int id = transfer.readInt();
            String sql = transfer.readString();
            int old = session.getModificationId();
            if (operation == Session.COMMAND_REPLICATION_UPDATE)
                session.setReplicationName(transfer.readString());

            PreparedStatement command = session.prepareStatement(sql, -1);
            cache.addObject(id, command);
            executeUpdate(command, operation, old);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE: {
            session.setAutoCommit(false);
            session.setRoot(false);
        }
        case Session.COMMAND_PREPARED_UPDATE:
        case Session.COMMAND_REPLICATION_PREPARED_UPDATE: {
            int id = transfer.readInt();
            if (operation == Session.COMMAND_REPLICATION_PREPARED_UPDATE)
                session.setReplicationName(transfer.readString());
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            setParameters(command);
            int old = session.getModificationId();
            executeUpdate(command, operation, old);
            break;
        }
        case Session.COMMAND_STORAGE_DISTRIBUTED_PUT: {
            session.setAutoCommit(false);
            session.setRoot(false);
        }
        case Session.COMMAND_STORAGE_PUT:
        case Session.COMMAND_STORAGE_REPLICATION_PUT: {
            String mapName = transfer.readString();
            byte[] key = transfer.readBytes();
            byte[] value = transfer.readBytes();
            int old = session.getModificationId();
            if (operation == Session.COMMAND_STORAGE_REPLICATION_PUT)
                session.setReplicationName(transfer.readString());

            StorageMap<Object, Object> map = session.getStorageMap(mapName);

            DataType valueType = map.getValueType();
            // synchronized (session) {
            Object result = map
                    .put(map.getKeyType().read(ByteBuffer.wrap(key)), valueType.read(ByteBuffer.wrap(value)));
            // }
            int status;
            if (session.isClosed()) {
                status = Session.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_PUT)
                transfer.writeString(session.getTransaction().getLocalTransactionNames());

            WriteBuffer writeBuffer = WriteBufferPool.poll();
            valueType.write(writeBuffer, result);
            ByteBuffer buffer = writeBuffer.getBuffer();
            buffer.flip();
            WriteBufferPool.offer(writeBuffer);
            transfer.writeByteBuffer(buffer);
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_DISTRIBUTED_GET: {
            session.setAutoCommit(false);
            session.setRoot(false);
        }
        case Session.COMMAND_STORAGE_GET: {
            String mapName = transfer.readString();
            byte[] key = transfer.readBytes();
            int old = session.getModificationId();

            StorageMap<Object, Object> map = session.getStorageMap(mapName);

            DataType valueType = map.getValueType();
            // synchronized (session) {
            Object result = map.get(map.getKeyType().read(ByteBuffer.wrap(key)));
            // }

            int status;
            if (session.isClosed()) {
                status = Session.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_PUT)
                transfer.writeString(session.getTransaction().getLocalTransactionNames());

            WriteBuffer writeBuffer = WriteBufferPool.poll();
            valueType.write(writeBuffer, result);
            ByteBuffer buffer = writeBuffer.getBuffer();
            buffer.flip();
            WriteBufferPool.offer(writeBuffer);
            transfer.writeByteBuffer(buffer);
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_MOVE_LEAF_PAGE: {
            String mapName = transfer.readString();
            ByteBuffer splitKey = transfer.readByteBuffer();
            ByteBuffer page = transfer.readByteBuffer();
            int old = session.getModificationId();
            StorageMap<Object, Object> map = session.getStorageMap(mapName);

            if (map instanceof Replication) {
                ((Replication) map).addLeafPage(splitKey, page);
            }

            int status;
            if (session.isClosed()) {
                status = Session.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_REMOVE_LEAF_PAGE: {
            String mapName = transfer.readString();
            ByteBuffer key = transfer.readByteBuffer();
            int old = session.getModificationId();
            StorageMap<Object, Object> map = session.getStorageMap(mapName);

            if (map instanceof Replication) {
                ((Replication) map).removeLeafPage(key);
            }

            int status;
            if (session.isClosed()) {
                status = Session.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            transfer.flush();
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
                writeColumn(result, i);
            }
            transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_COMMIT: {
            int old = session.getModificationId();
            synchronized (session) {
                session.commit(false, transfer.readString());
            }
            int status;
            if (session.isClosed()) {
                status = Session.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK: {
            int old = session.getModificationId();
            synchronized (session) {
                session.rollback();
            }
            int status;
            if (session.isClosed()) {
                status = Session.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT:
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT: {
            int old = session.getModificationId();
            String name = transfer.readString();
            synchronized (session) {
                if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT)
                    session.addSavepoint(name);
                else
                    session.rollbackToSavepoint(name);
            }
            int status;
            if (session.isClosed()) {
                status = Session.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE: {
            int old = session.getModificationId();
            boolean isValid = session.validateTransaction(transfer.readString());
            int status;
            if (session.isClosed()) {
                status = Session.STATUS_CLOSED;
            } else {
                status = getState(old);
            }
            transfer.writeInt(status);
            transfer.writeBoolean(isValid);
            transfer.flush();
            break;
        }
        case Session.COMMAND_BATCH_STATEMENT_UPDATE: {
            int size = transfer.readInt();
            ArrayList<String> batchCommands = New.arrayList(size);
            for (int i = 0; i < size; i++)
                batchCommands.add(transfer.readString());

            BatchStatement command = session.getBatchStatement(batchCommands);
            executeBatch(size, command);
            break;
        }
        case Session.COMMAND_BATCH_STATEMENT_PREPARED_UPDATE: {
            int id = transfer.readInt();
            int size = transfer.readInt();
            PreparedStatement preparedCommand = (PreparedStatement) cache.getObject(id, false);
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
            BatchStatement command = session.getBatchStatement(preparedCommand, batchParameters);
            executeBatch(size, command);
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
            writeRow(result, count);
            transfer.flush();
            break;
        }
        case Session.RESULT_RESET: {
            int id = transfer.readInt();
            Result result = (Result) cache.getObject(id, false);
            result.reset();
            break;
        }
        case Session.RESULT_CHANGE_ID: {
            int oldId = transfer.readInt();
            int newId = transfer.readInt();
            Object obj = cache.getObject(oldId, false);
            cache.freeObject(oldId);
            cache.addObject(newId, obj);
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
        case Session.SESSION_SET_ID: {
            sessionId = transfer.readString();
            transfer.writeInt(Session.STATUS_OK);
            transfer.writeBoolean(session.isAutoCommit());
            transfer.flush();
            break;
        }
        case Session.SESSION_SET_AUTO_COMMIT: {
            boolean autoCommit = transfer.readBoolean();
            session.setAutoCommit(autoCommit);
            transfer.writeInt(Session.STATUS_OK).flush();
            break;
        }
        case Session.SESSION_CLOSE: {
            stop = true;
            closeSession();
            transfer.writeInt(Session.STATUS_OK).flush();
            close();
            break;
        }
        case Session.COMMAND_READ_LOB: {
            if (lobs == null) {
                lobs = SmallLRUCache.newInstance(Math.max(SysProperties.SERVER_CACHED_OBJECTS,
                        SysProperties.SERVER_RESULT_SET_FETCH_SIZE * 5));
            }
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
                LobStorage lobStorage = session.getDataHandler().getLobStorage();
                // only the lob id is used
                ValueLob lob = ValueLob.create(Value.BLOB, null, -1, lobId, hmac, -1);
                InputStream lobIn = lobStorage.getInputStream(lob, hmac, -1);
                in = new CachedInputStream(lobIn);
                lobs.put(lobId, in);
                lobIn.skip(offset);
            }
            // limit the buffer size
            length = Math.min(16 * Constants.IO_BUFFER_SIZE, length);
            byte[] buff = new byte[length];
            length = IOUtils.readFully(in, buff, length);
            transfer.writeInt(Session.STATUS_OK);
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

    /**
     * An input stream with a position.
     */
    private static class CachedInputStream extends FilterInputStream {

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

}

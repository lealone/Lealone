/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.net;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.exceptions.JdbcSQLException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.common.util.SmallMap;
import org.lealone.common.util.StringUtils;
import org.lealone.db.CommandParameter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLob;
import org.lealone.replication.Replication;
import org.lealone.sql.PreparedStatement;
import org.lealone.storage.LobStorage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.WriteBuffer;
import org.lealone.storage.type.WriteBufferPool;

/**
 * An async tcp connection.
 * 
 * @author H2 Group
 * @author zhh
 */
public class AsyncConnection implements Comparable<AsyncConnection>, Handler<Buffer> {
    private static final Logger logger = LoggerFactory.getLogger(AsyncConnection.class);

    private final SmallMap cache = new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private SmallLRUCache<Long, CachedInputStream> lobs; // 大多数情况下都不使用lob，所以延迟初始化

    private final NetSocket socket;

    private boolean stop;

    private final ConcurrentHashMap<Integer, AsyncCallback<?>> callbackMap = new ConcurrentHashMap<>();

    private String baseDir;
    private boolean ifExists;

    private ConnectionInfo ci;

    private final ConcurrentHashMap<Integer, Session> sessions = new ConcurrentHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger(0);

    public int getNextId() {
        return nextId.incrementAndGet();
    }

    public void addSession(int sessionId, Session session) {
        sessions.put(sessionId, session);
    }

    public void remove(int sessionId) {
        sessions.remove(sessionId);
    }

    public boolean isEmpty() {
        return sessions.isEmpty();
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public void addAsyncCallback(int id, AsyncCallback<?> ac) {
        callbackMap.put(id, ac);
    }

    public AsyncCallback<?> getAsyncCallback(int id) {
        return callbackMap.get(id);
    }

    public AsyncConnection(NetSocket socket) {
        this.socket = socket;
    }

    public Transfer createTransfer(Session session) {
        Transfer t = new Transfer(this, socket);
        t.setSession(session);
        t.init();
        return t;
    }

    public void writeInitPacket(Session session, int sessionId, Transfer transfer, ConnectionInfo ci) throws Exception {
        transfer.setSSL(ci.isSSL());
        transfer.init();
        transfer.writeRequestHeader(sessionId, Session.SESSION_INIT);
        transfer.writeInt(Constants.TCP_PROTOCOL_VERSION_1); // minClientVersion
        transfer.writeInt(Constants.TCP_PROTOCOL_VERSION_1); // maxClientVersion
        transfer.writeString(ci.getDatabaseName());
        transfer.writeString(ci.getURL()); // 不带参数的URL
        transfer.writeString(ci.getUserName());
        transfer.writeBytes(ci.getUserPasswordHash());
        transfer.writeBytes(ci.getFilePasswordHash());
        transfer.writeBytes(ci.getFileEncryptionKey());
        String[] keys = ci.getKeys();
        transfer.writeInt(keys.length);
        for (String key : keys) {
            transfer.writeString(key).writeString(ci.getProperty(key));
        }
        VoidAsyncCallback ac = new VoidAsyncCallback() {
            @Override
            public void runInternal() {
                try {
                    int clientVersion = transfer.readInt();
                    transfer.setVersion(clientVersion);
                    boolean autoCommit = transfer.readBoolean();
                    session.setAutoCommit(autoCommit);
                } catch (IOException e) {
                    throw DbException.convert(e);
                }
            }
        };
        transfer.addAsyncCallback(sessionId, ac);
        transfer.flush();
        ac.await();
    }

    private void readInitPacket(Transfer transfer, int sessionId) {
        try {
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
            String userName = transfer.readString();
            userName = StringUtils.toUpperEnglish(userName);
            Session session = createSession(transfer, originalURL, dbName, userName);
            sessions.put(sessionId, session);
            transfer.setSession(session);
            transfer.writeResponseHeader(sessionId, Session.STATUS_OK);
            transfer.writeInt(clientVersion);
            transfer.writeBoolean(session.isAutoCommit());
            transfer.flush();
        } catch (Throwable e) {
            sendError(transfer, sessionId, e);
            stop = true;
        }
    }

    @Override
    public int compareTo(AsyncConnection o) {
        return socket.remoteAddress().host().compareTo(o.socket.remoteAddress().host());
    }

    private Session createSession(Transfer transfer, String originalURL, String dbName, String userName)
            throws IOException {
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

        if (baseDir == null) {
            baseDir = SysProperties.getBaseDirSilently();
        }

        // override client's requested properties with server settings
        if (baseDir != null) {
            ci.setBaseDir(baseDir);
        }
        if (ifExists) {
            ci.setProperty("IFEXISTS", "TRUE");
        }
        this.ci = ci;
        return createSession();
    }

    private Session createSession() {
        try {
            Session session = ci.getSessionFactory().createSession(ci);
            if (ci.getProperty("IS_LOCAL") != null)
                session.setLocal(Boolean.parseBoolean(ci.getProperty("IS_LOCAL")));
            else
                session.setLocal(true);
            return session;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private Session getSession(int sessionId) {
        Session session = sessions.get(sessionId);
        if (session == null) {
            throw DbException.convert(new RuntimeException("session not found: " + sessionId));
        }
        return session;
    }

    private void closeSession(Session session) {
        if (session != null) {
            try {
                session.prepareStatement("ROLLBACK", -1).executeUpdate();
                session.close();
            } catch (Exception e) {
                logger.error("Failed to close session", e);
            }
        }
    }

    /**
     * Close a connection.
     */
    void close() {
        try {
            stop = true;
            for (Session s : sessions.values())
                closeSession(s);
            sessions.clear();
        } catch (Exception e) {
            logger.error("Failed to close connection", e);
        }
    }

    private void setParameters(Transfer transfer, PreparedStatement command) throws IOException {
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
    private void writeParameterMetaData(Transfer transfer, CommandParameter p) throws IOException {
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
    private void writeColumn(Transfer transfer, Result result, int i) throws IOException {
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

    private void writeRow(Transfer transfer, Result result, int count) throws IOException {
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

    private int getState(Session session, int oldModificationId) {
        if (session.getModificationId() == oldModificationId) {
            return Session.STATUS_OK;
        }
        return Session.STATUS_OK_STATE_CHANGED;
    }

    private void writeBatchResult(Transfer transfer, Session session, int id, int[] result, int oldModificationId)
            throws IOException {
        writeResponseHeader(transfer, session, id, oldModificationId);
        for (int i = 0; i < result.length; i++)
            transfer.writeInt(result[i]);

        transfer.flush();
    }

    final ConcurrentLinkedQueue<PreparedCommand> preparedCommandQueue = new ConcurrentLinkedQueue<>();

    private void executeQuery(Transfer transfer, Session session, int id, PreparedStatement command, int operation,
            int objectId, int maxRows, int fetchSize, int oldModificationId) throws IOException {
        PreparedCommand pc = new PreparedCommand(id, command, transfer, session, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                final Result result = command.executeQueryAsync(maxRows);
                cache.addObject(objectId, result);

                Callable<Object> callable = new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        transfer.writeResponseHeader(id, getState(session, oldModificationId));

                        if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY
                                || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY)
                            transfer.writeString(session.getTransaction().getLocalTransactionNames());

                        int columnCount = result.getVisibleColumnCount();
                        transfer.writeInt(columnCount);
                        int rowCount = result.getRowCount();
                        transfer.writeInt(rowCount);
                        for (int i = 0; i < columnCount; i++) {
                            writeColumn(transfer, result, i);
                        }
                        int fetch = fetchSize;
                        if (rowCount != -1)
                            fetch = Math.min(rowCount, fetchSize);
                        writeRow(transfer, result, fetch);
                        transfer.flush();
                        return null;
                    }
                };
                session.setCallable(callable);
                prepareCommit(session, command);
                return null;
            }
        });

        preparedCommandQueue.add(pc);
        CommandHandler.preparedCommandQueue.add(pc);
    }

    private void prepareCommit(Session session, PreparedStatement command) throws Exception {
        if (!command.isTransactional()) {
            session.prepareCommit(true);
        } else if (session.isAutoCommit()) {
            session.prepareCommit(false);
        } else {
            // 当前语句是在一个手动提交的事务中进行，提前返回语句的执行结果
            session.getCallable().call();
        }
    }

    private void executeUpdate(Transfer transfer, Session session, int id, PreparedStatement command, int operation,
            int oldModificationId) throws IOException {
        PreparedCommand pc = new PreparedCommand(id, command, transfer, session, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                int updateCount = command.executeUpdateAsync();
                Callable<Object> callable = new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        int status;
                        if (session.isClosed()) {
                            status = Session.STATUS_CLOSED;
                        } else {
                            status = getState(session, oldModificationId);
                        }
                        transfer.writeResponseHeader(id, status);
                        if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE
                                || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE)
                            transfer.writeString(session.getTransaction().getLocalTransactionNames());

                        transfer.writeInt(updateCount);
                        transfer.flush();
                        return null;
                    }
                };
                session.setCallable(callable);
                prepareCommit(session, command);
                return null;
            }
        });

        preparedCommandQueue.add(pc);
        CommandHandler.preparedCommandQueue.add(pc);
    }

    private void sendError(Transfer transfer, int id, Throwable t) {
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

            transfer.writeResponseHeader(id, Session.STATUS_ERROR);
            transfer.writeString(e.getSQLState()).writeString(message).writeString(sql).writeInt(e.getErrorCode())
                    .writeString(trace).flush();
        } catch (Exception e2) {
            // if writing the error does not work, close the connection
            stop = true;
        }
    }

    private DbException parseError(Transfer transfer) {
        Throwable t;
        try {
            String sqlstate = transfer.readString();
            String message = transfer.readString();
            String sql = transfer.readString();
            int errorCode = transfer.readInt();
            String stackTrace = transfer.readString();
            JdbcSQLException s = new JdbcSQLException(message, sql, sqlstate, errorCode, null, stackTrace);
            t = s;
            if (errorCode == ErrorCode.CONNECTION_BROKEN_1) {
                IOException e = new IOException(s.toString());
                e.initCause(s);
                t = e;
            }
        } catch (Exception e) {
            t = e;
        }
        return DbException.convert(t);
    }

    private void processResponse(Transfer transfer, int id) throws IOException {
        int status = transfer.readInt();
        DbException e = null;
        if (status == Session.STATUS_ERROR) {
            e = parseError(transfer);
        } else if (status == Session.STATUS_CLOSED) {
            transfer = null;
        } else if (status == Session.STATUS_OK_STATE_CHANGED) {
            // sessionStateChanged = true;
        } else if (status == Session.STATUS_OK) {
            // ok
        } else {
            e = DbException.get(ErrorCode.CONNECTION_BROKEN_1, "unexpected status " + status);
        }

        AsyncCallback<?> ac = callbackMap.remove(id);
        if (ac == null) {
            logger.warn("Async callback is null, may be a bug! id = " + id);
            return;
        }
        if (e != null)
            ac.setDbException(e);
        else
            ac.run(transfer);
    }

    private void writeResponseHeader(Transfer transfer, Session session, int id, int oldModificationId)
            throws IOException {
        int status;
        if (session != null && session.isClosed()) {
            status = Session.STATUS_CLOSED;
        } else {
            status = getState(session, oldModificationId);
        }
        transfer.writeResponseHeader(id, status);
    }

    private void processRequest(Transfer transfer, int id) throws IOException {
        int operation = transfer.readInt();
        switch (operation) {
        case Session.SESSION_INIT: {
            readInitPacket(transfer, id);
            break;
        }
        case Session.COMMAND_PREPARE_READ_PARAMS:
        case Session.COMMAND_PREPARE: {
            int sessionId = transfer.readInt();
            String sql = transfer.readString();
            Session session = getSession(sessionId);
            int old = session.getModificationId();
            PreparedStatement command = session.prepareStatement(sql, -1);
            cache.addObject(id, command);
            boolean isQuery = command.isQuery();
            writeResponseHeader(transfer, session, id, old);
            transfer.writeBoolean(isQuery);
            if (operation == Session.COMMAND_PREPARE_READ_PARAMS) {
                List<? extends CommandParameter> params = command.getParameters();
                transfer.writeInt(params.size());
                for (CommandParameter p : params) {
                    writeParameterMetaData(transfer, p);
                }
            }
            transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY:
        case Session.COMMAND_QUERY: {
            int sessionId = transfer.readInt();
            String sql = transfer.readString();
            int objectId = transfer.readInt();
            int maxRows = transfer.readInt();
            int fetchSize = transfer.readInt();
            Session session = getSession(sessionId);
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            int old = session.getModificationId();
            PreparedStatement command = session.prepareStatement(sql, fetchSize);
            cache.addObject(id, command);
            executeQuery(transfer, session, id, command, operation, objectId, maxRows, fetchSize, old);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY:
        case Session.COMMAND_PREPARED_QUERY: {
            int sessionId = transfer.readInt();
            int objectId = transfer.readInt();
            int maxRows = transfer.readInt();
            int fetchSize = transfer.readInt();
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            command.setFetchSize(fetchSize);
            setParameters(transfer, command);
            Session session = getSession(sessionId);
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            int old = session.getModificationId();
            executeQuery(transfer, session, id, command, operation, objectId, maxRows, fetchSize, old);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE:
        case Session.COMMAND_UPDATE:
        case Session.COMMAND_REPLICATION_UPDATE: {
            int sessionId = transfer.readInt();
            String sql = transfer.readString();
            Session session = getSession(sessionId);
            int old = session.getModificationId();
            if (operation == Session.COMMAND_REPLICATION_UPDATE)
                session.setReplicationName(transfer.readString());
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            PreparedStatement command = session.prepareStatement(sql, -1);
            cache.addObject(id, command);
            executeUpdate(transfer, session, id, command, operation, old);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE:
        case Session.COMMAND_PREPARED_UPDATE:
        case Session.COMMAND_REPLICATION_PREPARED_UPDATE: {
            int sessionId = transfer.readInt();
            Session session = getSession(sessionId);
            if (operation == Session.COMMAND_REPLICATION_PREPARED_UPDATE)
                session.setReplicationName(transfer.readString());
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            setParameters(transfer, command);
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            int old = session.getModificationId();
            executeUpdate(transfer, session, id, command, operation, old);
            break;
        }
        case Session.COMMAND_STORAGE_DISTRIBUTED_PUT:
        case Session.COMMAND_STORAGE_PUT:
        case Session.COMMAND_STORAGE_REPLICATION_PUT: {
            int sessionId = transfer.readInt();
            String mapName = transfer.readString();
            byte[] key = transfer.readBytes();
            byte[] value = transfer.readBytes();
            Session session = getSession(sessionId);
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_PUT) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            int old = session.getModificationId();
            if (operation == Session.COMMAND_STORAGE_REPLICATION_PUT)
                session.setReplicationName(transfer.readString());

            StorageMap<Object, Object> map = session.getStorageMap(mapName);

            DataType valueType = map.getValueType();
            Object k = map.getKeyType().read(ByteBuffer.wrap(key));
            Object v = valueType.read(ByteBuffer.wrap(value));
            Object result = map.put(k, v);
            writeResponseHeader(transfer, session, id, old);
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
        case Session.COMMAND_STORAGE_DISTRIBUTED_GET:
        case Session.COMMAND_STORAGE_GET: {
            int sessionId = transfer.readInt();
            String mapName = transfer.readString();
            byte[] key = transfer.readBytes();
            Session session = getSession(sessionId);
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_GET) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            int old = session.getModificationId();

            StorageMap<Object, Object> map = session.getStorageMap(mapName);

            DataType valueType = map.getValueType();
            Object result = map.get(map.getKeyType().read(ByteBuffer.wrap(key)));

            writeResponseHeader(transfer, session, id, old);
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
            int sessionId = transfer.readInt();
            String mapName = transfer.readString();
            ByteBuffer splitKey = transfer.readByteBuffer();
            ByteBuffer page = transfer.readByteBuffer();
            Session session = getSession(sessionId);
            int old = session.getModificationId();
            StorageMap<Object, Object> map = session.getStorageMap(mapName);

            if (map instanceof Replication) {
                ((Replication) map).addLeafPage(splitKey, page);
            }

            writeResponseHeader(transfer, session, id, old);
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_REMOVE_LEAF_PAGE: {
            int sessionId = transfer.readInt();
            String mapName = transfer.readString();
            ByteBuffer key = transfer.readByteBuffer();
            Session session = getSession(sessionId);
            int old = session.getModificationId();
            StorageMap<Object, Object> map = session.getStorageMap(mapName);

            if (map instanceof Replication) {
                ((Replication) map).removeLeafPage(key);
            }

            writeResponseHeader(transfer, session, id, old);
            transfer.flush();
            break;
        }
        case Session.COMMAND_GET_META_DATA: {
            int objectId = transfer.readInt();
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            Result result = command.getMetaData();
            cache.addObject(objectId, result);
            int columnCount = result.getVisibleColumnCount();
            transfer.writeResponseHeader(id, Session.STATUS_OK);
            transfer.writeInt(columnCount).writeInt(0);
            for (int i = 0; i < columnCount; i++) {
                writeColumn(transfer, result, i);
            }
            transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_COMMIT: {
            int sessionId = transfer.readInt();
            Session session = getSession(sessionId);
            int old = session.getModificationId();
            session.commit(false, transfer.readString());
            writeResponseHeader(transfer, session, id, old);
            transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK: {
            int sessionId = transfer.readInt();
            Session session = getSession(sessionId);
            int old = session.getModificationId();
            session.rollback();
            writeResponseHeader(transfer, session, id, old);
            transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT:
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT: {
            int sessionId = transfer.readInt();
            Session session = getSession(sessionId);
            int old = session.getModificationId();
            String name = transfer.readString();
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT)
                session.addSavepoint(name);
            else
                session.rollbackToSavepoint(name);
            writeResponseHeader(transfer, session, id, old);
            transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE: {
            int sessionId = transfer.readInt();
            Session session = getSession(sessionId);
            int old = session.getModificationId();
            boolean isValid = session.validateTransaction(transfer.readString());
            writeResponseHeader(transfer, session, id, old);
            transfer.writeBoolean(isValid);
            transfer.flush();
            break;
        }
        case Session.COMMAND_BATCH_STATEMENT_UPDATE: {
            int sessionId = transfer.readInt();
            Session session = getSession(sessionId);
            int size = transfer.readInt();
            int[] result = new int[size];
            int old = session.getModificationId();
            for (int i = 0; i < size; i++) {
                String sql = transfer.readString();
                PreparedStatement command = session.prepareStatement(sql, -1);
                try {
                    result[i] = command.executeUpdate();
                } catch (Exception e) {
                    result[i] = Statement.EXECUTE_FAILED;
                }
            }
            writeBatchResult(transfer, session, id, result, old);
            break;
        }
        case Session.COMMAND_BATCH_STATEMENT_PREPARED_UPDATE: {
            int sessionId = transfer.readInt();
            int size = transfer.readInt();
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            List<? extends CommandParameter> params = command.getParameters();
            int paramsSize = params.size();
            int[] result = new int[size];
            Session session = getSession(sessionId);
            int old = session.getModificationId();
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < paramsSize; j++) {
                    CommandParameter p = params.get(j);
                    p.setValue(transfer.readValue());
                }
                try {
                    result[i] = command.executeUpdate();
                } catch (Exception e) {
                    result[i] = Statement.EXECUTE_FAILED;
                }
            }
            writeBatchResult(transfer, session, id, result, old);
            break;
        }
        case Session.COMMAND_CLOSE: {
            PreparedStatement command = (PreparedStatement) cache.getObject(id, true);
            if (command != null) {
                command.close();
                cache.freeObject(id);
            }
            break;
        }
        case Session.RESULT_FETCH_ROWS: {
            int count = transfer.readInt();
            Result result = (Result) cache.getObject(id, false);
            transfer.writeResponseHeader(id, Session.STATUS_OK);
            writeRow(transfer, result, count);
            transfer.flush();
            break;
        }
        case Session.RESULT_RESET: {
            Result result = (Result) cache.getObject(id, false);
            result.reset();
            break;
        }
        case Session.RESULT_CHANGE_ID: {
            int oldId = id;
            int newId = transfer.readInt();
            Object obj = cache.getObject(oldId, false);
            cache.freeObject(oldId);
            cache.addObject(newId, obj);
            break;
        }
        case Session.RESULT_CLOSE: {
            Result result = (Result) cache.getObject(id, true);
            if (result != null) {
                result.close();
                cache.freeObject(id);
            }
            break;
        }
        case Session.SESSION_SET_AUTO_COMMIT: {
            int sessionId = transfer.readInt();
            boolean autoCommit = transfer.readBoolean();
            Session session = getSession(sessionId);
            session.setAutoCommit(autoCommit);
            transfer.writeResponseHeader(id, Session.STATUS_OK).flush();
            break;
        }
        case Session.SESSION_CLOSE: {
            Session session = sessions.remove(id);
            closeSession(session);
            break;
        }
        case Session.SESSION_CANCEL_STATEMENT: {
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            if (command != null) {
                command.cancel();
                command.close();
                cache.freeObject(id);
            }
            break;
        }
        case Session.COMMAND_READ_LOB: {
            int sessionId = transfer.readInt();
            Session session = getSession(sessionId);
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
            transfer.writeResponseHeader(id, Session.STATUS_OK);
            transfer.writeInt(length);
            transfer.writeBytes(buff, 0, length);
            transfer.flush();
            break;
        }
        default:
            logger.warn("Unknow operation: {}", operation);
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

    private Buffer tmpBuffer;
    private final ConcurrentLinkedQueue<Buffer> bufferQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void handle(Buffer buffer) {
        if (tmpBuffer != null) {
            buffer = tmpBuffer.appendBuffer(buffer);
            tmpBuffer = null;
        }

        int length = buffer.length();
        if (length < 4) {
            tmpBuffer = buffer;
            return;
        }
        int pos = 0;
        while (true) {
            int packetLength = buffer.getInt(pos);
            if (length - 4 == packetLength) {
                if (pos == 0) {
                    bufferQueue.offer(buffer);
                } else {
                    Buffer b = Buffer.buffer();
                    b.appendBuffer(buffer, pos, packetLength + 4);
                    bufferQueue.offer(b);
                }
                break;
            } else if (length - 4 > packetLength) {
                Buffer b = Buffer.buffer();
                b.appendBuffer(buffer, pos, packetLength + 4);
                bufferQueue.offer(b);

                pos = pos + packetLength + 4;
                length = length - (packetLength + 4);

                // 有可能剩下的不够4个字节了
                if (length < 4) {
                    tmpBuffer = Buffer.buffer();
                    tmpBuffer.appendBuffer(buffer, pos, length);
                    break;
                } else {
                    continue;
                }
            } else {
                tmpBuffer = Buffer.buffer();
                tmpBuffer.appendBuffer(buffer, pos, length);
                break;
            }
        }

        parsePackets();
    }

    private void parsePackets() {
        while (!stop) {
            Buffer buffer = bufferQueue.poll();
            if (buffer == null)
                return;
            try {
                Transfer transfer = new Transfer(this, socket);
                transfer.setBuffer(buffer);
                transfer.readInt(); // packetLength

                boolean isRequest = transfer.readByte() == Transfer.REQUEST;
                int id = transfer.readInt();
                // boolean isRequest = (id & 1) == 0;
                // id = id >>> 1;
                if (isRequest) {
                    try {
                        processRequest(transfer, id);
                    } catch (Throwable e) {
                        // logger.error("Process request", e);
                        sendError(transfer, id, e);
                    }
                } else {
                    processResponse(transfer, id);
                }
            } catch (Throwable e) {
                logger.error("Parse packets", e);
            }
        }
    }

    public void executeOneCommand() {
        PreparedCommand c = preparedCommandQueue.poll();
        if (c == null)
            return;
        try {
            c.run();
        } catch (Throwable e) {
            sendError(c.transfer, c.id, e);
        }
    }
}

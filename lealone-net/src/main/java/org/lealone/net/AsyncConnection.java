/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.net;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.concurrent.ConcurrentUtils;
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
import org.lealone.db.DataBuffer;
import org.lealone.db.RunMode;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLob;
import org.lealone.db.value.ValueLong;
import org.lealone.sql.PreparedStatement;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.LobStorage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

/**
 * An async tcp connection.
 * 
 * @author H2 Group
 * @author zhh
 */
public class AsyncConnection {

    static class SessionInfo {
        final String hostAndPort;
        final int sessionId;
        final Session session;
        final CommandHandler commandHandler;
        final ConcurrentLinkedQueue<PreparedCommand> preparedCommandQueue;
        final long commandHandlerSequence;

        SessionInfo(String hostAndPort, int sessionId, Session session, CommandHandler commandHandler) {
            this.sessionId = sessionId;
            this.hostAndPort = hostAndPort;
            this.session = session;
            this.commandHandler = commandHandler;
            this.preparedCommandQueue = new ConcurrentLinkedQueue<>();

            // SessionInfo的hostAndPort和sessionId字段不足以区别它自身，所以用commandHandlerSequence
            commandHandlerSequence = commandHandler.getNextSequence();
        }

        @Override
        public String toString() {
            return "SessionInfo [hostAndPort=" + hostAndPort + ", sessionId=" + sessionId + "]";
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(AsyncConnection.class);

    private final SmallMap cache = new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private SmallLRUCache<Long, CachedInputStream> lobs; // 大多数情况下都不使用lob，所以延迟初始化
    private String baseDir;

    protected final WritableChannel writableChannel;
    private final boolean isServer;
    private InetSocketAddress inetSocketAddress;
    private final ConcurrentHashMap<Integer, AsyncCallback<?>> callbackMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Session> sessions = new ConcurrentHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger(0);
    final ConcurrentHashMap<Integer, SessionInfo> sessionInfoMap = new ConcurrentHashMap<>();
    protected String hostAndPort;
    private NetClient netClient;

    public int getNextId() {
        return nextId.incrementAndGet();
    }

    public void addSession(int sessionId, Session session) {
        sessions.put(sessionId, session);
    }

    public void removeSession(int sessionId) {
        sessions.remove(sessionId);
        if (netClient != null && sessions.isEmpty()) {
            netClient.removeConnection(inetSocketAddress);
        }
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    public void addAsyncCallback(int id, AsyncCallback<?> ac) {
        callbackMap.put(id, ac);
    }

    public AsyncCallback<?> getAsyncCallback(int id) {
        return callbackMap.get(id);
    }

    public WritableChannel getWritableChannel() {
        return writableChannel;
    }

    public AsyncConnection(WritableChannel writableChannel, boolean isServer) {
        this.writableChannel = writableChannel;
        this.isServer = isServer;
    }

    public AsyncConnection(WritableChannel writableChannel, boolean isServer, NetClient netClient) {
        this.writableChannel = writableChannel;
        this.isServer = isServer;
        this.netClient = netClient;
    }

    public Transfer createTransfer(Session session) {
        return new Transfer(this, writableChannel, session);
    }

    public void writeInitPacket(Session session, Transfer transfer, ConnectionInfo ci) throws Exception {
        int id = session.getNextId();
        transfer.setSSL(ci.isSSL());
        transfer.writeRequestHeader(id, Session.SESSION_INIT);
        transfer.writeInt(Constants.TCP_PROTOCOL_VERSION_1); // minClientVersion
        transfer.writeInt(Constants.TCP_PROTOCOL_VERSION_1); // maxClientVersion
        transfer.writeString(hostAndPort);
        transfer.writeString(ci.getDatabaseShortName());
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
        AsyncCallback<Void> ac = new AsyncCallback<Void>() {
            @Override
            public void runInternal() {
                try {
                    int clientVersion = transfer.readInt();
                    transfer.setVersion(clientVersion);
                    boolean autoCommit = transfer.readBoolean();
                    session.setAutoCommit(autoCommit);
                    session.setTargetEndpoints(transfer.readString());
                    session.setRunMode(RunMode.valueOf(transfer.readString()));
                    session.setInvalid(transfer.readBoolean());
                } catch (IOException e) {
                    throw DbException.convert(e);
                }
            }
        };
        transfer.addAsyncCallback(id, ac);
        transfer.flush();
        ac.await();
    }

    private void readInitPacket(Transfer transfer, int id, int sessionId) {
        try {
            int minClientVersion = transfer.readInt();
            if (minClientVersion < Constants.TCP_PROTOCOL_VERSION_MIN) {
                throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + minClientVersion,
                        "" + Constants.TCP_PROTOCOL_VERSION_MIN);
            } else if (minClientVersion > Constants.TCP_PROTOCOL_VERSION_MAX) {
                throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + minClientVersion,
                        "" + Constants.TCP_PROTOCOL_VERSION_MAX);
            }
            int clientVersion;
            int maxClientVersion = transfer.readInt();
            if (maxClientVersion >= Constants.TCP_PROTOCOL_VERSION_MAX) {
                clientVersion = Constants.TCP_PROTOCOL_VERSION_CURRENT;
            } else {
                clientVersion = minClientVersion;
            }
            transfer.setVersion(clientVersion);
            hostAndPort = transfer.readString();
            String dbName = transfer.readString();
            String originalURL = transfer.readString();
            String userName = transfer.readString();
            userName = StringUtils.toUpperEnglish(userName);
            Session session = createSession(transfer, originalURL, dbName, userName);
            if (session.isValid()) {
                CommandHandler commandHandler = CommandHandler.getNextCommandHandler();
                sessions.put(sessionId, session);
                SessionInfo sessionInfo = new SessionInfo(hostAndPort, sessionId, session, commandHandler);
                sessionInfoMap.put(sessionId, sessionInfo);
                commandHandler.addSession(sessionInfo);
            }
            transfer.setSession(session);
            transfer.writeResponseHeader(id, Session.STATUS_OK);
            transfer.writeInt(clientVersion);
            transfer.writeBoolean(session.isAutoCommit());
            transfer.writeString(session.getTargetEndpoints());
            transfer.writeString(session.getRunMode().toString());
            transfer.writeBoolean(session.isInvalid());
            transfer.flush();
        } catch (Throwable e) {
            sendError(transfer, id, e);
        }
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
        return ci.createSession();
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
    protected void close() {
        try {
            for (Session s : sessions.values())
                closeSession(s);
            sessions.clear();
            for (SessionInfo sessionInfo : sessionInfoMap.values()) {
                sessionInfo.commandHandler.removeSession(sessionInfo);
            }
            sessionInfoMap.clear();
        } catch (Exception e) {
            logger.error("Failed to close connection", e);
        }
    }

    private static void setParameters(Transfer transfer, PreparedStatement command) throws IOException {
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
    private static void writeParameterMetaData(Transfer transfer, CommandParameter p) throws IOException {
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
    private static void writeColumn(Transfer transfer, Result result, int i) throws IOException {
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

    private static void writeRow(Transfer transfer, Result result, int count) throws IOException {
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

    private static int getStatus(Session session) {
        if (session.isClosed()) {
            return Session.STATUS_CLOSED;
        } else if (session.isRunModeChanged()) {
            return Session.STATUS_RUN_MODE_CHANGED;
        } else {
            return Session.STATUS_OK;
        }
    }

    private static void writeBatchResult(Transfer transfer, Session session, int id, int[] result) throws IOException {
        writeResponseHeader(transfer, session, id);
        for (int i = 0; i < result.length; i++)
            transfer.writeInt(result[i]);

        transfer.flush();
    }

    private void executeQueryAsync(Transfer transfer, Session session, int sessionId, int id, PreparedStatement command,
            int operation, int objectId, int maxRows, int fetchSize) throws IOException {
        PreparedCommand pc = new PreparedCommand(id, command, transfer, session, new Runnable() {
            @Override
            public void run() {
                command.executeQueryAsync(maxRows, false, res -> {
                    if (res.isSucceeded()) {
                        Result result = res.getResult();
                        cache.addObject(objectId, result);
                        try {
                            transfer.writeResponseHeader(id, getStatus(session));
                            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY
                                    || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY) {
                                transfer.writeString(session.getTransaction().getLocalTransactionNames());
                            }
                            if (session.isRunModeChanged()) {
                                transfer.writeInt(sessionId).writeString(session.getNewTargetEndpoints());
                            }
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
                        } catch (Exception e) {
                            sendError(transfer, id, e);
                        }
                    } else {
                        sendError(transfer, id, res.getCause());
                    }
                });
            }
        });
        addPreparedCommandToQueue(pc, sessionId);
    }

    private void executeUpdateAsync(Transfer transfer, Session session, int sessionId, int id,
            PreparedStatement command, int operation) throws IOException {
        PreparedCommand pc = new PreparedCommand(id, command, transfer, session, new Runnable() {
            @Override
            public void run() {
                command.executeUpdateAsync(res -> {
                    if (res.isSucceeded()) {
                        int updateCount = res.getResult();
                        try {
                            transfer.writeResponseHeader(id, getStatus(session));
                            if (session.isRunModeChanged()) {
                                transfer.writeInt(sessionId).writeString(session.getNewTargetEndpoints());
                            }
                            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE
                                    || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE) {
                                transfer.writeString(session.getTransaction().getLocalTransactionNames());
                            }
                            transfer.writeInt(updateCount);
                            transfer.writeLong(session.getLastRowKey());
                            transfer.flush();
                        } catch (Exception e) {
                            sendError(transfer, id, e);
                        }
                    } else {
                        sendError(transfer, id, res.getCause());
                    }
                });
            }
        });
        addPreparedCommandToQueue(pc, sessionId);
    }

    private void addPreparedCommandToQueue(PreparedCommand pc, int sessionId) {
        SessionInfo sessionInfo = sessionInfoMap.get(sessionId);
        if (sessionInfo == null) {
            throw DbException.throwInternalError("sessionInfo is null");
        }

        sessionInfo.preparedCommandQueue.add(pc);
        sessionInfo.commandHandler.wakeUp();
    }

    protected void sendError(Transfer transfer, int id, Throwable t) {
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

            if (isServer) {
                message = "[Server] " + message;
            }

            transfer.reset(); // 为什么要reset? 见reset中的注释

            transfer.writeResponseHeader(id, Session.STATUS_ERROR);
            transfer.writeString(e.getSQLState()).writeString(message).writeString(sql).writeInt(e.getErrorCode())
                    .writeString(trace).flush();
        } catch (Exception e2) {
            if (transfer.getSession() != null)
                transfer.getSession().close();
            else if (writableChannel != null) {
                writableChannel.close();
            }
            logger.error("Failed to send error", e2);
        }
    }

    private static DbException parseError(Transfer transfer) {
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
        String newTargetEndpoints = null;
        Session session = null;
        int status = transfer.readInt();
        DbException e = null;
        if (status == Session.STATUS_OK) {
            // ok
        } else if (status == Session.STATUS_ERROR) {
            e = parseError(transfer);
        } else if (status == Session.STATUS_CLOSED) {
            transfer = null;
        } else if (status == Session.STATUS_RUN_MODE_CHANGED) {
            int sessionId = transfer.readInt();
            session = sessions.get(sessionId);
            newTargetEndpoints = transfer.readString();
        } else {
            e = DbException.get(ErrorCode.CONNECTION_BROKEN_1, "unexpected status " + status);
        }

        AsyncCallback<?> ac = callbackMap.remove(id);
        if (ac == null) {
            String msg = "Async callback is null, may be a bug! id = " + id;
            if (e != null) {
                logger.warn(msg, e);
            } else {
                logger.warn(msg);
            }
            return;
        }
        if (e != null)
            ac.setDbException(e);
        ac.run(transfer);
        if (newTargetEndpoints != null)
            session.runModeChanged(newTargetEndpoints);
    }

    private static void writeResponseHeader(Transfer transfer, Session session, int id) throws IOException {
        transfer.writeResponseHeader(id, getStatus(session));
    }

    protected void processRequest(Transfer transfer, int id, int operation) throws IOException {
        int sessionId = transfer.readInt();
        Session session = sessions.get(sessionId);
        // 初始化时，肯定是不存在session的
        if (session == null && operation != Session.SESSION_INIT) {
            throw DbException.convert(new RuntimeException("session not found: " + sessionId));
        }
        transfer.setSession(session);

        switch (operation) {
        case Session.SESSION_INIT: {
            readInitPacket(transfer, id, sessionId);
            break;
        }
        case Session.COMMAND_PREPARE_READ_PARAMS:
        case Session.COMMAND_PREPARE: {
            String sql = transfer.readString();
            PreparedStatement command = session.prepareStatement(sql, -1);
            cache.addObject(id, command);
            boolean isQuery = command.isQuery();
            writeResponseHeader(transfer, session, id);
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
            String sql = transfer.readString();
            int objectId = transfer.readInt();
            int maxRows = transfer.readInt();
            int fetchSize = transfer.readInt();
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            PreparedStatement command = session.prepareStatement(sql, fetchSize);
            command.setFetchSize(fetchSize);
            cache.addObject(id, command);
            executeQueryAsync(transfer, session, sessionId, id, command, operation, objectId, maxRows, fetchSize);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY:
        case Session.COMMAND_PREPARED_QUERY: {
            int objectId = transfer.readInt();
            int maxRows = transfer.readInt();
            int fetchSize = transfer.readInt();
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            command.setFetchSize(fetchSize);
            setParameters(transfer, command);
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            executeQueryAsync(transfer, session, sessionId, id, command, operation, objectId, maxRows, fetchSize);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE:
        case Session.COMMAND_UPDATE:
        case Session.COMMAND_REPLICATION_UPDATE: {
            String sql = transfer.readString();
            if (operation == Session.COMMAND_REPLICATION_UPDATE) {
                session.setReplicationName(transfer.readString());
                session.setRoot(false);
            }
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            PreparedStatement command = session.prepareStatement(sql, -1);
            cache.addObject(id, command);
            executeUpdateAsync(transfer, session, sessionId, id, command, operation);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE:
        case Session.COMMAND_PREPARED_UPDATE:
        case Session.COMMAND_REPLICATION_PREPARED_UPDATE: {
            if (operation == Session.COMMAND_REPLICATION_PREPARED_UPDATE)
                session.setReplicationName(transfer.readString());
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            setParameters(transfer, command);
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            executeUpdateAsync(transfer, session, sessionId, id, command, operation);
            break;
        }
        case Session.COMMAND_REPLICATION_COMMIT: {
            long validKey = transfer.readLong();
            boolean autoCommit = transfer.readBoolean();
            session.replicationCommit(validKey, autoCommit);
            break;
        }
        case Session.COMMAND_REPLICATION_ROLLBACK: {
            session.rollback();
            break;
        }
        case Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_PUT:
        case Session.COMMAND_STORAGE_PUT:
        case Session.COMMAND_STORAGE_REPLICATION_PUT: {
            String mapName = transfer.readString();
            byte[] key = transfer.readBytes();
            byte[] value = transfer.readBytes();
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_PUT) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            // if (operation == Session.COMMAND_STORAGE_REPLICATION_PUT)
            // session.setReplicationName(transfer.readString());
            session.setReplicationName(transfer.readString());
            boolean raw = transfer.readBoolean();

            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            if (raw) {
                map = map.getRawMap();
            }

            StorageDataType valueType = map.getValueType();
            Object k = map.getKeyType().read(ByteBuffer.wrap(key));
            Object v = valueType.read(ByteBuffer.wrap(value));
            Object result = map.put(k, v);
            writeResponseHeader(transfer, session, id);
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_PUT)
                transfer.writeString(session.getTransaction().getLocalTransactionNames());

            if (result != null) {
                try (DataBuffer writeBuffer = DataBuffer.create()) {
                    valueType.write(writeBuffer, result);
                    ByteBuffer buffer = writeBuffer.getAndFlipBuffer();
                    transfer.writeByteBuffer(buffer);
                }
            } else {
                transfer.writeByteBuffer(null);
            }
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_APPEND:
        case Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_APPEND: {
            String mapName = transfer.readString();
            byte[] value = transfer.readBytes();
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_APPEND) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            session.setReplicationName(transfer.readString());

            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            Object v = map.getValueType().read(ByteBuffer.wrap(value));
            Object result = map.append(v);
            writeResponseHeader(transfer, session, id);
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_APPEND)
                transfer.writeString(session.getTransaction().getLocalTransactionNames());
            transfer.writeLong(((ValueLong) result).getLong());
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_GET:
        case Session.COMMAND_STORAGE_GET: {
            String mapName = transfer.readString();
            byte[] key = transfer.readBytes();
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_GET) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }

            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            Object result = map.get(map.getKeyType().read(ByteBuffer.wrap(key)));

            writeResponseHeader(transfer, session, id);
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_GET)
                transfer.writeString(session.getTransaction().getLocalTransactionNames());

            if (result != null) {
                try (DataBuffer writeBuffer = DataBuffer.create()) {
                    map.getValueType().write(writeBuffer, result);
                    ByteBuffer buffer = writeBuffer.getAndFlipBuffer();
                    transfer.writeByteBuffer(buffer);
                }
            } else {
                transfer.writeByteBuffer(null);
            }
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_PREPARE_MOVE_LEAF_PAGE: {
            String mapName = transfer.readString();
            LeafPageMovePlan leafPageMovePlan = LeafPageMovePlan.deserialize(transfer);

            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            leafPageMovePlan = map.prepareMoveLeafPage(leafPageMovePlan);
            writeResponseHeader(transfer, session, id);
            leafPageMovePlan.serialize(transfer);
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_MOVE_LEAF_PAGE: {
            String mapName = transfer.readString();
            ByteBuffer splitKey = transfer.readByteBuffer();
            ByteBuffer page = transfer.readByteBuffer();
            boolean last = transfer.readBoolean();
            boolean addPage = transfer.readBoolean();
            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            ConcurrentUtils.submitTask("Add Leaf Page", () -> {
                map.addLeafPage(splitKey, page, last, addPage);
            });
            // map.addLeafPage(splitKey, page, last, addPage);
            // writeResponseHeader(transfer, session, id);
            // transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_REPLICATE_ROOT_PAGES: {
            final String dbName = transfer.readString();
            final ByteBuffer rootPages = transfer.readByteBuffer();
            final Session s = session;
            ConcurrentUtils.submitTask("Replicate Root Pages", () -> {
                s.replicateRootPages(dbName, rootPages);
            });

            // session.replicateRootPages(dbName, rootPages);
            // writeResponseHeader(transfer, session, id);
            // transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_READ_PAGE: {
            String mapName = transfer.readString();
            ByteBuffer splitKey = transfer.readByteBuffer();
            boolean last = transfer.readBoolean();

            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            ByteBuffer page = map.readPage(splitKey, last);
            writeResponseHeader(transfer, session, id);
            transfer.writeByteBuffer(page);
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_REMOVE_LEAF_PAGE: {
            String mapName = transfer.readString();
            ByteBuffer key = transfer.readByteBuffer();

            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            map.removeLeafPage(key);
            writeResponseHeader(transfer, session, id);
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
            session.commit(transfer.readString());
            // writeResponseHeader(transfer, session, id); //不需要发回响应
            // transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK: {
            session.rollback();
            // writeResponseHeader(transfer, session, id); //不需要发回响应
            // transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT:
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT: {
            String name = transfer.readString();
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT)
                session.addSavepoint(name);
            else
                session.rollbackToSavepoint(name);
            // writeResponseHeader(transfer, session, id); //不需要发回响应
            // transfer.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE: {
            boolean isValid = session.validateTransaction(transfer.readString());
            writeResponseHeader(transfer, session, id);
            transfer.writeBoolean(isValid);
            transfer.flush();
            break;
        }
        case Session.COMMAND_BATCH_STATEMENT_UPDATE: {
            int size = transfer.readInt();
            int[] result = new int[size];
            for (int i = 0; i < size; i++) {
                String sql = transfer.readString();
                PreparedStatement command = session.prepareStatement(sql, -1);
                try {
                    result[i] = command.executeUpdate();
                } catch (Exception e) {
                    result[i] = Statement.EXECUTE_FAILED;
                }
            }
            writeBatchResult(transfer, session, id, result);
            break;
        }
        case Session.COMMAND_BATCH_STATEMENT_PREPARED_UPDATE: {
            int size = transfer.readInt();
            PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
            List<? extends CommandParameter> params = command.getParameters();
            int paramsSize = params.size();
            int[] result = new int[size];
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
            writeBatchResult(transfer, session, id, result);
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
            boolean autoCommit = transfer.readBoolean();
            session.setAutoCommit(autoCommit);
            transfer.writeResponseHeader(id, Session.STATUS_OK).flush();
            break;
        }
        case Session.SESSION_CLOSE: {
            SessionInfo si = sessionInfoMap.remove(sessionId);
            if (si != null) {
                si.commandHandler.removeSession(si);
                session = sessions.remove(sessionId);
                closeSession(session);
            } else {
                logger.warn("SessionInfo is null, may be a bug! sessionId = " + sessionId);
            }
            break;
        }
        case Session.SESSION_CANCEL_STATEMENT: {
            int statementId = transfer.readInt();
            PreparedStatement command = (PreparedStatement) cache.getObject(statementId, false);
            if (command != null) {
                command.cancel();
                command.close();
                cache.freeObject(statementId);
            }
            break;
        }
        case Session.COMMAND_READ_LOB: {
            if (lobs == null) {
                lobs = SmallLRUCache.newInstance(
                        Math.max(SysProperties.SERVER_CACHED_OBJECTS, SysProperties.SERVER_RESULT_SET_FETCH_SIZE * 5));
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

    private NetBuffer lastBuffer;

    public void handle(NetBuffer buffer) {
        if (lastBuffer != null) {
            buffer = lastBuffer.appendBuffer(buffer);
            lastBuffer = null;
        }

        int length = buffer.length();
        if (length < 4) {
            lastBuffer = buffer;
            return;
        }

        int pos = 0;
        try {
            while (true) {
                // 必须生成新的Transfer实例，不同协议包对应不同Transfer实例，
                // 否则如果有多个CommandHandler线程时会用同一个Transfer实例写数据，这会产生并发问题。
                Transfer transfer;
                if (pos == 0)
                    transfer = new Transfer(this, writableChannel, buffer);
                else
                    transfer = new Transfer(this, writableChannel, buffer.slice(pos, pos + length));
                int packetLength = transfer.readInt();
                if (length - 4 == packetLength) {
                    parsePacket(transfer);
                    break;
                } else if (length - 4 > packetLength) {
                    parsePacket(transfer);
                    pos = pos + packetLength + 4;
                    length = length - (packetLength + 4);
                    // 有可能剩下的不够4个字节了
                    if (length < 4) {
                        lastBuffer = buffer.getBuffer(pos, pos + length);
                        break;
                    } else {
                        continue;
                    }
                } else {
                    lastBuffer = buffer.getBuffer(pos, pos + length);
                    break;
                }
            }
        } catch (Throwable e) {
            if (isServer)
                logger.error("Parse packet", e);
        }
    }

    private void parsePacket(Transfer transfer) throws IOException {
        boolean isRequest = transfer.readByte() == Transfer.REQUEST;
        int id = transfer.readInt();
        if (isRequest) {
            int operation = transfer.readInt();
            try {
                processRequest(transfer, id, operation);
            } catch (Throwable e) {
                logger.error("Failed to process request, operation: " + operation, e);
                sendError(transfer, id, e);
            }
        } else {
            processResponse(transfer, id);
        }
    }

    public String getHostAndPort() {
        return hostAndPort;
    }

    public void setHostAndPort(String hostAndPort) {
        this.hostAndPort = hostAndPort;
    }

    public InetSocketAddress getInetSocketAddress() {
        return inetSocketAddress;
    }

    public void setInetSocketAddress(InetSocketAddress inetSocketAddress) {
        this.inetSocketAddress = inetSocketAddress;
    }
}

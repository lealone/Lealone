/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.server;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.common.util.SmallMap;
import org.lealone.db.CommandParameter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.DataBuffer;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLob;
import org.lealone.db.value.ValueLong;
import org.lealone.net.Transfer;
import org.lealone.net.TransferConnection;
import org.lealone.net.WritableChannel;
import org.lealone.server.Scheduler.PreparedCommand;
import org.lealone.server.Scheduler.SessionInfo;
import org.lealone.sql.PreparedStatement;
import org.lealone.storage.DistributedStorageMap;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.LobStorage;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

/**
 * 这里只处理客户端通过TCP连到服务器端后的协议，可以在一个TCP连接中打开多个session
 * 
 */
// 注意: 以下代码中出现的sessionId都表示客户端session的id，
// 调用createSession创建的是服务器端的session，这个session的id有可能跟客户端session的id不一样，
// 但是可以把客户端session的id跟服务器端的session做一个影射，这样两端的session就对上了。
public class TcpServerConnection extends TransferConnection {

    private static final Logger logger = LoggerFactory.getLogger(TcpServerConnection.class);

    // 每个sessionId对应一个专有的SessionInfo，
    // 所有与这个sessionId相关的命令请求都先放到SessionInfo中的队列，
    // 然后由调度器根据优先级从多个队列中依次取出执行。
    private final ConcurrentHashMap<Integer, SessionInfo> sessions = new ConcurrentHashMap<>();
    private final SmallMap cache = new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private final TcpServer tcpServer;
    private SmallLRUCache<Long, CachedInputStream> lobs; // 大多数情况下都不使用lob，所以延迟初始化

    public TcpServerConnection(TcpServer tcpServer, WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
        this.tcpServer = tcpServer;
    }

    protected SmallMap getCache() {
        return cache;
    }

    /**
     * @see org.lealone.net.TcpClientConnection#writeInitPacket
     */
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

            ConnectionInfo ci = createConnectionInfo(transfer);
            Session session = createSession(ci, sessionId);

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

    private ConnectionInfo createConnectionInfo(Transfer transfer) throws IOException {
        String dbName = transfer.readString();
        String originalURL = transfer.readString();
        String userName = transfer.readString();
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
        ci.initTraceProperty();

        String baseDir = tcpServer.getBaseDir();
        if (baseDir == null) {
            baseDir = SysProperties.getBaseDirSilently();
        }
        // 强制使用服务器端的基目录
        if (baseDir != null) {
            ci.setBaseDir(baseDir);
        }
        return ci;
    }

    private Session createSession(ConnectionInfo ci, int sessionId) {
        Session session = ci.createSession();
        // 在复制模式和sharding模式下，客户端可以从任何一个节点接入，
        // 如果接入节点不是客户端想要访问的数据库的所在节点，就会给客户端返回数据库的所有节点，
        // 此时，这样的session就是无效的，客户端会自动重定向到正确的节点。
        if (session.isValid()) {
            // 每个sessionId对应一个SessionInfo，每个调度器可以负责多个SessionInfo， 但是一个SessionInfo只能由一个调度器负责。
            // sessions这个字段并没有考虑放到调度器中，这样做的话光有sessionId作为key是不够的，
            // 还需要当前连接做限定，因为每个连接可以接入多个客户端session，不同连接中的sessionId是可以相同的，
            // 把sessions这个字段放在连接实例中可以减少并发访问的冲突。
            SessionInfo si = new SessionInfo(this, session, sessionId, tcpServer.getSessionTimeout());
            sessions.put(sessionId, si);
        }
        return session;
    }

    private SessionInfo getSessionInfo(int sessionId) {
        return sessions.get(sessionId);
    }

    private void sessionNotFound(Transfer transfer, int id, int sessionId) {
        String msg = "Server session not found, maybe closed or timeout. client session id: " + sessionId;
        RuntimeException e = new RuntimeException(msg);
        // logger.warn(msg, e); //打印错误堆栈不是很大必要
        logger.warn(msg);
        sendError(transfer, id, e);
    }

    private void closeSession(Transfer transfer, int id, int sessionId) {
        SessionInfo si = getSessionInfo(sessionId);
        if (si != null) {
            closeSession(si);
        } else {
            sessionNotFound(transfer, id, sessionId);
        }
    }

    void closeSession(SessionInfo si) {
        try {
            si.session.prepareStatement("ROLLBACK", -1).executeUpdate();
            si.session.close();
        } catch (Exception e) {
            logger.error("Failed to close session", e);
        } finally {
            si.remove();
            sessions.remove(si.sessionId);
        }
    }

    @Override
    public void close() {
        super.close();
        for (SessionInfo si : sessions.values()) {
            closeSession(si);
        }
        sessions.clear();
    }

    protected static void setParameters(Transfer transfer, PreparedStatement command) throws IOException {
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

    protected static void writeResponseHeader(Transfer transfer, Session session, int id) throws IOException {
        transfer.writeResponseHeader(id, getStatus(session));
    }

    protected static List<PageKey> readPageKeys(Transfer transfer) throws IOException {
        ArrayList<PageKey> pageKeys;
        int size = transfer.readInt();
        if (size > 0) {
            pageKeys = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                PageKey pk = transfer.readPageKey();
                pageKeys.add(pk);
            }
        } else {
            pageKeys = null;
        }
        return pageKeys;
    }

    protected void executeQueryAsync(Transfer transfer, int id, int operation, Session session, int sessionId,
            boolean prepared) throws IOException {
        int resultId = transfer.readInt();
        int maxRows = transfer.readInt();
        int fetchSize = transfer.readInt();
        boolean scrollable = transfer.readBoolean();

        if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY
                || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY) {
            session.setAutoCommit(false);
            session.setRoot(false);
        }

        PreparedStatement stmt;
        if (prepared) {
            stmt = (PreparedStatement) cache.getObject(id, false);
            setParameters(transfer, stmt);
        } else {
            String sql = transfer.readString();
            stmt = session.prepareStatement(sql, fetchSize);
            cache.addObject(id, stmt);
        }
        stmt.setFetchSize(fetchSize);

        List<PageKey> pageKeys = readPageKeys(transfer);
        if (executeQueryAsync(transfer, id, operation, session, sessionId, stmt, resultId, fetchSize)) {
            return;
        }
        PreparedStatement.Yieldable<?> yieldable = stmt.createYieldableQuery(maxRows, scrollable, ar -> {
            if (ar.isSucceeded()) {
                Result result = ar.getResult();
                sendResult(transfer, id, operation, session, sessionId, result, resultId, fetchSize);
            } else {
                sendError(transfer, id, ar.getCause());
            }
        });
        yieldable.setPageKeys(pageKeys);

        addPreparedCommandToQueue(transfer, id, session, sessionId, stmt, yieldable);
    }

    protected boolean executeQueryAsync(Transfer transfer, int id, int operation, Session session, int sessionId,
            PreparedStatement stmt, int resultId, int fetchSize) throws IOException {
        return false;
    }

    protected void sendResult(Transfer transfer, int id, int operation, Session session, int sessionId, Result result,
            int resultId, int fetchSize) {
        cache.addObject(resultId, result);
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
    }

    protected void executeUpdateAsync(Transfer transfer, int id, int operation, Session session, int sessionId,
            boolean prepared) throws IOException {
        if (operation == Session.COMMAND_REPLICATION_UPDATE
                || operation == Session.COMMAND_REPLICATION_PREPARED_UPDATE) {
            session.setReplicationName(transfer.readString());
        } else if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE
                || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE) {
            session.setAutoCommit(false);
            session.setRoot(false);
        }

        PreparedStatement stmt;
        if (prepared) {
            stmt = (PreparedStatement) cache.getObject(id, false);
            setParameters(transfer, stmt);
        } else {
            String sql = transfer.readString();
            stmt = session.prepareStatement(sql, -1);
            cache.addObject(id, stmt);
        }

        List<PageKey> pageKeys = readPageKeys(transfer);

        PreparedStatement.Yieldable<?> yieldable = stmt.createYieldableUpdate(ar -> {
            if (ar.isSucceeded()) {
                int updateCount = ar.getResult();
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
                sendError(transfer, id, ar.getCause());
            }
        });
        yieldable.setPageKeys(pageKeys);

        addPreparedCommandToQueue(transfer, id, session, sessionId, stmt, yieldable);
    }

    private void addPreparedCommandToQueue(Transfer transfer, int id, Session session, int sessionId,
            PreparedStatement stmt, PreparedStatement.Yieldable<?> yieldable) {
        SessionInfo si = getSessionInfo(sessionId);
        if (si == null) {
            sessionNotFound(transfer, id, sessionId);
            return;
        }
        PreparedCommand pc = new PreparedCommand(transfer, id, session, stmt, yieldable, si);
        si.addCommand(pc);
    }

    @Override
    protected void handleRequest(Transfer transfer, int id, int operation) throws IOException {
        Scheduler scheduler;
        Session session;

        // 参数id是数据包的id，而这里的sessionId是客户端session的id，每个数据包都会带这两个字段
        int sessionId = transfer.readInt();
        SessionInfo si = getSessionInfo(sessionId);
        if (si == null) {
            // 创建新session时临时分配一个调度器，当新session创建成功后再分配一个固定的调度器，
            // 之后此session相关的请求包和命令都由固定的调度器负责处理。
            if (operation == Session.SESSION_INIT) {
                scheduler = ScheduleService.getScheduler();
            } else {
                sessionNotFound(transfer, id, sessionId);
                return;
            }
            session = null;
        } else {
            si.updateLastTime();
            scheduler = si.getScheduler();
            session = si.session;
            transfer.setSession(session);
        }
        AsyncTask task = new RequestPacketDeliveryTask(this, transfer, id, operation, session, sessionId);
        scheduler.handle(task);
    }

    private static class RequestPacketDeliveryTask implements AsyncTask {
        final TcpServerConnection conn;
        final Transfer transfer;
        final int id;
        final int operation;
        final Session session;
        final int sessionId;

        public RequestPacketDeliveryTask(TcpServerConnection conn, Transfer transfer, int id, int operation,
                Session session, int sessionId) {
            this.conn = conn;
            this.transfer = transfer;
            this.id = id;
            this.operation = operation;
            this.session = session;
            this.sessionId = sessionId;
        }

        @Override
        public int getPriority() {
            return NORM_PRIORITY;
        }

        @Override
        public void run() {
            try {
                conn.handleRequest(transfer, id, operation, session, sessionId);
            } catch (Throwable e) {
                logger.error("Failed to handle request, id: " + id + ", operation: " + operation, e);
                conn.sendError(transfer, id, e);
            }
        }
    }

    private void handleRequest(Transfer transfer, int id, int operation, Session session, int sessionId)
            throws IOException {
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
            executeQueryAsync(transfer, id, operation, session, sessionId, false);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY:
        case Session.COMMAND_PREPARED_QUERY: {
            executeQueryAsync(transfer, id, operation, session, sessionId, true);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE:
        case Session.COMMAND_UPDATE:
        case Session.COMMAND_REPLICATION_UPDATE: {
            executeUpdateAsync(transfer, id, operation, session, sessionId, false);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE:
        case Session.COMMAND_PREPARED_UPDATE:
        case Session.COMMAND_REPLICATION_PREPARED_UPDATE: {
            executeUpdateAsync(transfer, id, operation, session, sessionId, true);
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

            DistributedStorageMap<Object, Object> map = (DistributedStorageMap<Object, Object>) session
                    .getStorageMap(mapName);
            leafPageMovePlan = map.prepareMoveLeafPage(leafPageMovePlan);
            writeResponseHeader(transfer, session, id);
            leafPageMovePlan.serialize(transfer);
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_MOVE_LEAF_PAGE: {
            String mapName = transfer.readString();
            PageKey pageKey = transfer.readPageKey();
            ByteBuffer page = transfer.readByteBuffer();
            boolean addPage = transfer.readBoolean();
            DistributedStorageMap<Object, Object> map = (DistributedStorageMap<Object, Object>) session
                    .getStorageMap(mapName);
            ConcurrentUtils.submitTask("Add Leaf Page", () -> {
                map.addLeafPage(pageKey, page, addPage);
            });
            // map.addLeafPage(pageKey, page, addPage);
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
            PageKey pageKey = transfer.readPageKey();
            DistributedStorageMap<Object, Object> map = (DistributedStorageMap<Object, Object>) session
                    .getStorageMap(mapName);
            ByteBuffer page = map.readPage(pageKey);
            writeResponseHeader(transfer, session, id);
            transfer.writeByteBuffer(page);
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_REMOVE_LEAF_PAGE: {
            String mapName = transfer.readString();
            PageKey pageKey = transfer.readPageKey();

            DistributedStorageMap<Object, Object> map = (DistributedStorageMap<Object, Object>) session
                    .getStorageMap(mapName);
            map.removeLeafPage(pageKey);
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
            closeSession(transfer, id, sessionId);
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
}

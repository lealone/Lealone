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
import java.util.function.Function;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.ExpiringMap;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.Pair;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.db.CommandParameter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.DataBuffer;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLob;
import org.lealone.db.value.ValueLong;
import org.lealone.net.TransferConnection;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.net.WritableChannel;
import org.lealone.server.Scheduler.PreparedCommand;
import org.lealone.server.Scheduler.SessionInfo;
import org.lealone.sql.PreparedSQLStatement;
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
    private final TcpServer tcpServer;
    private final ExpiringMap<Integer, AutoCloseable> cache;
    private SmallLRUCache<Long, CachedInputStream> lobs; // 大多数情况下都不使用lob，所以延迟初始化

    public TcpServerConnection(TcpServer tcpServer, WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
        this.tcpServer = tcpServer;
        cache = new ExpiringMap<>(ScheduleService.getScheduler(), tcpServer.getSessionTimeout(),
                new Function<Pair<Integer, ExpiringMap.CacheableObject<AutoCloseable>>, Void>() {
                    @Override
                    public Void apply(Pair<Integer, ExpiringMap.CacheableObject<AutoCloseable>> pair) {
                        try {
                            pair.right.value.close();
                        } catch (Exception e) {
                            logger.warn(e.getMessage());
                        }
                        return null;
                    }
                });
    }

    /**
     * @see org.lealone.net.TcpClientConnection#writeInitPacket
     */
    private void readInitPacket(TransferInputStream in, int packetId, int sessionId) {
        try {
            int minClientVersion = in.readInt();
            if (minClientVersion < Constants.TCP_PROTOCOL_VERSION_MIN) {
                throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + minClientVersion,
                        "" + Constants.TCP_PROTOCOL_VERSION_MIN);
            } else if (minClientVersion > Constants.TCP_PROTOCOL_VERSION_MAX) {
                throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2, "" + minClientVersion,
                        "" + Constants.TCP_PROTOCOL_VERSION_MAX);
            }
            int clientVersion;
            int maxClientVersion = in.readInt();
            if (maxClientVersion >= Constants.TCP_PROTOCOL_VERSION_MAX) {
                clientVersion = Constants.TCP_PROTOCOL_VERSION_CURRENT;
            } else {
                clientVersion = minClientVersion;
            }

            ConnectionInfo ci = createConnectionInfo(in);
            Session session = createSession(ci, sessionId);
            session.setProtocolVersion(clientVersion);
            in.setSession(session);

            TransferOutputStream out = createTransferOutputStream(session);
            out.writeResponseHeader(packetId, Session.STATUS_OK);
            out.writeInt(clientVersion);
            out.writeBoolean(session.isAutoCommit());
            out.writeString(session.getTargetNodes());
            out.writeString(session.getRunMode().toString());
            out.writeBoolean(session.isInvalid());
            out.flush();
        } catch (Throwable e) {
            sendError(null, packetId, e);
        }
    }

    private ConnectionInfo createConnectionInfo(TransferInputStream in) throws IOException {
        String dbName = in.readString();
        String originalURL = in.readString();
        String userName = in.readString();
        ConnectionInfo ci = new ConnectionInfo(originalURL, dbName);

        ci.setUserName(userName);
        ci.setUserPasswordHash(in.readBytes());
        ci.setFilePasswordHash(in.readBytes());
        ci.setFileEncryptionKey(in.readBytes());

        int len = in.readInt();
        for (int i = 0; i < len; i++) {
            String key = in.readString();
            String value = in.readString();
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

    private void sessionNotFound(int packetId, int sessionId) {
        String msg = "Server session not found, maybe closed or timeout. client session id: " + sessionId;
        RuntimeException e = new RuntimeException(msg);
        // logger.warn(msg, e); //打印错误堆栈不是很大必要
        logger.warn(msg);
        sendError(null, packetId, e);
    }

    private void closeSession(int packetId, int sessionId) {
        SessionInfo si = getSessionInfo(sessionId);
        if (si != null) {
            closeSession(si);
        } else {
            sessionNotFound(packetId, sessionId);
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
        cache.close();
    }

    protected static void readParameters(TransferInputStream in, PreparedSQLStatement command) throws IOException {
        int len = in.readInt();
        List<? extends CommandParameter> params = command.getParameters();
        for (int i = 0; i < len; i++) {
            CommandParameter p = params.get(i);
            p.setValue(in.readValue());
        }
    }

    /**
     * Write the parameter meta data to the transfer object.
     *
     * @param p the parameter
     */
    private static void writeParameterMetaData(TransferOutputStream out, CommandParameter p) throws IOException {
        out.writeInt(p.getType());
        out.writeLong(p.getPrecision());
        out.writeInt(p.getScale());
        out.writeInt(p.getNullable());
    }

    /**
     * Write a result column to the given output.
     *
     * @param result the result
     * @param i the column index
     */
    private static void writeColumn(TransferOutputStream out, Result result, int i) throws IOException {
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

    private static void writeRow(TransferOutputStream out, Result result, int count) throws IOException {
        try {
            int visibleColumnCount = result.getVisibleColumnCount();
            for (int i = 0; i < count; i++) {
                if (result.next()) {
                    out.writeBoolean(true);
                    Value[] v = result.currentRow();
                    for (int j = 0; j < visibleColumnCount; j++) {
                        out.writeValue(v[j]);
                    }
                } else {
                    out.writeBoolean(false);
                    break;
                }
            }
        } catch (Throwable e) {
            // 如果取结果集的下一行记录时发生了异常，
            // 结果集包必须加一个结束标记，结果集包后面跟一个异常包。
            out.writeBoolean(false);
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

    private static void writeBatchResult(TransferOutputStream out, Session session, int packetId, int[] result)
            throws IOException {
        writeResponseHeader(out, session, packetId);
        for (int i = 0; i < result.length; i++)
            out.writeInt(result[i]);

        out.flush();
    }

    protected static void writeResponseHeader(TransferOutputStream out, Session session, int packetId)
            throws IOException {
        out.writeResponseHeader(packetId, getStatus(session));
    }

    protected static List<PageKey> readPageKeys(TransferInputStream in) throws IOException {
        ArrayList<PageKey> pageKeys;
        int size = in.readInt();
        if (size > 0) {
            pageKeys = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                PageKey pk = in.readPageKey();
                pageKeys.add(pk);
            }
        } else {
            pageKeys = null;
        }
        return pageKeys;
    }

    protected void executeQueryAsync(TransferInputStream in, int packetId, int operation, SessionInfo si,
            boolean prepared) throws IOException {
        final Session session = si.session;
        final int sessionId = si.sessionId;

        int resultId = in.readInt();
        int maxRows = in.readInt();
        int fetchSize = in.readInt();
        boolean scrollable = in.readBoolean();

        if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY
                || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY) {
            session.setAutoCommit(false);
            session.setRoot(false);
        }

        List<PageKey> pageKeys = readPageKeys(in);
        PreparedSQLStatement stmt;
        if (prepared) {
            int commandId = in.readInt();
            stmt = (PreparedSQLStatement) cache.get(commandId);
            readParameters(in, stmt);
        } else {
            // 客户端的非Prepared语句不需要缓存
            String sql = in.readString();
            stmt = session.prepareStatement(sql, fetchSize);
            stmt.setId(packetId);
        }
        stmt.setFetchSize(fetchSize);

        // 允许其他扩展跳过正常的流程
        if (executeQueryAsync(packetId, operation, session, sessionId, stmt, resultId, fetchSize)) {
            return;
        }
        PreparedSQLStatement.Yieldable<?> yieldable = stmt.createYieldableQuery(maxRows, scrollable, ar -> {
            if (ar.isSucceeded()) {
                Result result = ar.getResult();
                sendResult(packetId, operation, session, sessionId, result, resultId, fetchSize);
            } else {
                sendError(session, packetId, ar.getCause());
            }
        });
        yieldable.setPageKeys(pageKeys);
        addPreparedCommandToQueue(packetId, si, stmt, yieldable);
    }

    protected boolean executeQueryAsync(int packetId, int operation, Session session, int sessionId,
            PreparedSQLStatement stmt, int resultId, int fetchSize) throws IOException {
        return false;
    }

    protected void sendResult(int packetId, int operation, Session session, int sessionId, Result result, int resultId,
            int fetchSize) {
        cache.put(resultId, result);
        try {
            TransferOutputStream out = createTransferOutputStream(session);
            out.writeResponseHeader(packetId, getStatus(session));
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY
                    || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY) {
                out.writeString(session.getTransaction().getLocalTransactionNames());
            }
            if (session.isRunModeChanged()) {
                out.writeInt(sessionId).writeString(session.getNewTargetNodes());
            }
            int columnCount = result.getVisibleColumnCount();
            out.writeInt(columnCount);
            int rowCount = result.getRowCount();
            out.writeInt(rowCount);
            for (int i = 0; i < columnCount; i++) {
                writeColumn(out, result, i);
            }
            int fetch = fetchSize;
            if (rowCount != -1)
                fetch = Math.min(rowCount, fetchSize);
            writeRow(out, result, fetch);
            out.flush();
        } catch (Exception e) {
            sendError(session, packetId, e);
        }
    }

    protected void executeUpdateAsync(TransferInputStream in, int packetId, int operation, SessionInfo si,
            boolean prepared) throws IOException {
        final Session session = si.session;
        final int sessionId = si.sessionId;

        if (operation == Session.COMMAND_REPLICATION_UPDATE
                || operation == Session.COMMAND_REPLICATION_PREPARED_UPDATE) {
            session.setReplicationName(in.readString());
        } else if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE
                || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE) {
            session.setAutoCommit(false);
            session.setRoot(false);
        }

        List<PageKey> pageKeys = readPageKeys(in);
        PreparedSQLStatement stmt;
        if (prepared) {
            int commandId = in.readInt();
            stmt = (PreparedSQLStatement) cache.get(commandId);
            readParameters(in, stmt);
        } else {
            // 客户端的非Prepared语句不需要缓存
            String sql = in.readString();
            stmt = session.prepareStatement(sql, -1);
            // 非Prepared语句执行一次就结束，所以可以用packetId当唯一标识，一般用来执行客户端发起的取消操作
            stmt.setId(packetId);
        }

        PreparedSQLStatement.Yieldable<?> yieldable = stmt.createYieldableUpdate(ar -> {
            if (ar.isSucceeded()) {
                int updateCount = ar.getResult();
                try {
                    TransferOutputStream out = createTransferOutputStream(session);
                    out.writeResponseHeader(packetId, getStatus(session));
                    if (session.isRunModeChanged()) {
                        out.writeInt(sessionId).writeString(session.getNewTargetNodes());
                    }
                    if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE
                            || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE) {
                        out.writeString(session.getTransaction().getLocalTransactionNames());
                    }
                    out.writeInt(updateCount);
                    out.writeLong(session.getLastRowKey());
                    out.flush();
                } catch (Exception e) {
                    sendError(session, packetId, e);
                }
            } else {
                sendError(session, packetId, ar.getCause());
            }
        });
        yieldable.setPageKeys(pageKeys);
        addPreparedCommandToQueue(packetId, si, stmt, yieldable);
    }

    private void addPreparedCommandToQueue(int packetId, SessionInfo si, PreparedSQLStatement stmt,
            PreparedSQLStatement.Yieldable<?> yieldable) {
        PreparedCommand pc = new PreparedCommand(this, packetId, si, stmt, yieldable);
        si.addCommand(pc);
    }

    // 这个方法是由网络事件循环线程执行的
    @Override
    protected void handleRequest(TransferInputStream in, int packetId, int operation) throws IOException {
        // 这里的sessionId是客户端session的id，每个数据包都会带这个字段
        int sessionId = in.readInt();
        SessionInfo si = getSessionInfo(sessionId);
        if (si == null) {
            // 创建新session时临时分配一个调度器，当新session创建成功后再分配一个固定的调度器，
            // 之后此session相关的请求包和命令都由固定的调度器负责处理。
            if (operation == Session.SESSION_INIT) {
                ScheduleService.getScheduler().handle(() -> {
                    try {
                        readInitPacket(in, packetId, sessionId);
                    } catch (Throwable e) {
                        logger.error("Failed to create session, packetId: " + packetId, e);
                        sendError(null, packetId, e);
                    }
                });
            } else {
                sessionNotFound(packetId, sessionId);
            }
        } else {
            si.updateLastTime();
            in.setSession(si.session);
            si.getScheduler().handle(() -> {
                try {
                    handleRequest(in, packetId, operation, si);
                } catch (Throwable e) {
                    logger.error("Failed to handle request, packetId: " + packetId + ", operation: " + operation, e);
                    sendError(si.session, packetId, e);
                } finally {
                    // in.closeInputStream(); // 到这里输入流已经读完，及时释放NetBuffer
                }
            });
        }
    }

    // 这个方法就已经是由调度器线程执行了
    private void handleRequest(TransferInputStream in, int packetId, int operation, SessionInfo si) throws IOException {
        Session session = si.session;
        int sessionId = si.sessionId;
        switch (operation) {
        case Session.COMMAND_PREPARE_READ_PARAMS:
        case Session.COMMAND_PREPARE: {
            int commandId = in.readInt();
            String sql = in.readString();
            PreparedSQLStatement command = session.prepareStatement(sql, -1);
            command.setId(commandId);
            cache.put(commandId, command);
            boolean isQuery = command.isQuery();
            TransferOutputStream out = createTransferOutputStream(session);
            writeResponseHeader(out, session, packetId);
            out.writeBoolean(isQuery);
            if (operation == Session.COMMAND_PREPARE_READ_PARAMS) {
                List<? extends CommandParameter> params = command.getParameters();
                out.writeInt(params.size());
                for (CommandParameter p : params) {
                    writeParameterMetaData(out, p);
                }
            }
            out.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY:
        case Session.COMMAND_QUERY: {
            executeQueryAsync(in, packetId, operation, si, false);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY:
        case Session.COMMAND_PREPARED_QUERY: {
            executeQueryAsync(in, packetId, operation, si, true);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE:
        case Session.COMMAND_UPDATE:
        case Session.COMMAND_REPLICATION_UPDATE: {
            executeUpdateAsync(in, packetId, operation, si, false);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE:
        case Session.COMMAND_PREPARED_UPDATE:
        case Session.COMMAND_REPLICATION_PREPARED_UPDATE: {
            executeUpdateAsync(in, packetId, operation, si, true);
            break;
        }
        case Session.COMMAND_REPLICATION_COMMIT: {
            long validKey = in.readLong();
            boolean autoCommit = in.readBoolean();
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
            String mapName = in.readString();
            byte[] key = in.readBytes();
            byte[] value = in.readBytes();
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_PUT) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            session.setReplicationName(in.readString());
            boolean raw = in.readBoolean();

            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            if (raw) {
                map = map.getRawMap();
            }

            StorageDataType valueType = map.getValueType();
            Object k = map.getKeyType().read(ByteBuffer.wrap(key));
            Object v = valueType.read(ByteBuffer.wrap(value));
            Object result = map.put(k, v);
            TransferOutputStream out = createTransferOutputStream(session);
            writeResponseHeader(out, session, packetId);
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_PUT)
                out.writeString(session.getTransaction().getLocalTransactionNames());

            if (result != null) {
                try (DataBuffer writeBuffer = DataBuffer.create()) {
                    valueType.write(writeBuffer, result);
                    ByteBuffer buffer = writeBuffer.getAndFlipBuffer();
                    out.writeByteBuffer(buffer);
                }
            } else {
                out.writeByteBuffer(null);
            }
            out.flush();
            break;
        }
        case Session.COMMAND_STORAGE_APPEND:
        case Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_APPEND: {
            String mapName = in.readString();
            byte[] value = in.readBytes();
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_APPEND) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }
            session.setReplicationName(in.readString());

            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            Object v = map.getValueType().read(ByteBuffer.wrap(value));
            Object result = map.append(v);
            TransferOutputStream out = createTransferOutputStream(session);
            writeResponseHeader(out, session, packetId);
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_APPEND)
                out.writeString(session.getTransaction().getLocalTransactionNames());
            out.writeLong(((ValueLong) result).getLong());
            out.flush();
            break;
        }
        case Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_GET:
        case Session.COMMAND_STORAGE_GET: {
            String mapName = in.readString();
            byte[] key = in.readBytes();
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_GET) {
                session.setAutoCommit(false);
                session.setRoot(false);
            }

            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            Object result = map.get(map.getKeyType().read(ByteBuffer.wrap(key)));

            TransferOutputStream out = createTransferOutputStream(session);
            writeResponseHeader(out, session, packetId);
            if (operation == Session.COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_GET)
                out.writeString(session.getTransaction().getLocalTransactionNames());

            if (result != null) {
                try (DataBuffer writeBuffer = DataBuffer.create()) {
                    map.getValueType().write(writeBuffer, result);
                    ByteBuffer buffer = writeBuffer.getAndFlipBuffer();
                    out.writeByteBuffer(buffer);
                }
            } else {
                out.writeByteBuffer(null);
            }
            out.flush();
            break;
        }
        case Session.COMMAND_STORAGE_PREPARE_MOVE_LEAF_PAGE: {
            String mapName = in.readString();
            LeafPageMovePlan leafPageMovePlan = LeafPageMovePlan.deserialize(in);

            DistributedStorageMap<Object, Object> map = (DistributedStorageMap<Object, Object>) session
                    .getStorageMap(mapName);
            leafPageMovePlan = map.prepareMoveLeafPage(leafPageMovePlan);
            TransferOutputStream out = createTransferOutputStream(session);
            writeResponseHeader(out, session, packetId);
            leafPageMovePlan.serialize(out);
            out.flush();
            break;
        }
        case Session.COMMAND_STORAGE_MOVE_LEAF_PAGE: {
            String mapName = in.readString();
            PageKey pageKey = in.readPageKey();
            ByteBuffer page = in.readByteBuffer();
            boolean addPage = in.readBoolean();
            DistributedStorageMap<Object, Object> map = (DistributedStorageMap<Object, Object>) session
                    .getStorageMap(mapName);
            ConcurrentUtils.submitTask("Add Leaf Page", () -> {
                map.addLeafPage(pageKey, page, addPage);
            });
            // map.addLeafPage(pageKey, page, addPage);
            // writeResponseHeader(out, session, packetId);
            // transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_REPLICATE_ROOT_PAGES: {
            final String dbName = in.readString();
            final ByteBuffer rootPages = in.readByteBuffer();
            final Session s = session;
            ConcurrentUtils.submitTask("Replicate Root Pages", () -> {
                s.replicateRootPages(dbName, rootPages);
            });

            // session.replicateRootPages(dbName, rootPages);
            // writeResponseHeader(out, session, packetId);
            // transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_READ_PAGE: {
            String mapName = in.readString();
            PageKey pageKey = in.readPageKey();
            DistributedStorageMap<Object, Object> map = (DistributedStorageMap<Object, Object>) session
                    .getStorageMap(mapName);
            ByteBuffer page = map.readPage(pageKey);
            TransferOutputStream out = createTransferOutputStream(session);
            writeResponseHeader(out, session, packetId);
            out.writeByteBuffer(page);
            out.flush();
            break;
        }
        case Session.COMMAND_STORAGE_REMOVE_LEAF_PAGE: {
            String mapName = in.readString();
            PageKey pageKey = in.readPageKey();

            DistributedStorageMap<Object, Object> map = (DistributedStorageMap<Object, Object>) session
                    .getStorageMap(mapName);
            map.removeLeafPage(pageKey);
            TransferOutputStream out = createTransferOutputStream(session);
            writeResponseHeader(out, session, packetId);
            out.flush();
            break;
        }
        case Session.COMMAND_GET_META_DATA: {
            int commandId = in.readInt();
            PreparedSQLStatement command = (PreparedSQLStatement) cache.get(commandId);
            Result result = command.getMetaData();
            int columnCount = result.getVisibleColumnCount();
            TransferOutputStream out = createTransferOutputStream(session);
            out.writeResponseHeader(packetId, Session.STATUS_OK);
            out.writeInt(columnCount);
            for (int i = 0; i < columnCount; i++) {
                writeColumn(out, result, i);
            }
            out.flush();
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_COMMIT: {
            session.commit(in.readString());
            // 不需要发回响应
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK: {
            session.rollback();
            // 不需要发回响应
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT:
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT: {
            String name = in.readString();
            if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT)
                session.addSavepoint(name);
            else
                session.rollbackToSavepoint(name);
            // 不需要发回响应
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE: {
            boolean isValid = session.validateTransaction(in.readString());
            TransferOutputStream out = createTransferOutputStream(session);
            writeResponseHeader(out, session, packetId);
            out.writeBoolean(isValid);
            out.flush();
            break;
        }
        case Session.COMMAND_BATCH_STATEMENT_UPDATE: {
            int size = in.readInt();
            int[] result = new int[size];
            for (int i = 0; i < size; i++) {
                String sql = in.readString();
                PreparedSQLStatement command = session.prepareStatement(sql, -1);
                try {
                    result[i] = command.executeUpdate();
                } catch (Exception e) {
                    result[i] = Statement.EXECUTE_FAILED;
                }
            }
            TransferOutputStream out = createTransferOutputStream(session);
            writeBatchResult(out, session, packetId, result);
            break;
        }
        case Session.COMMAND_BATCH_STATEMENT_PREPARED_UPDATE: {
            int commandId = in.readInt();
            int size = in.readInt();
            PreparedSQLStatement command = (PreparedSQLStatement) cache.get(commandId);
            List<? extends CommandParameter> params = command.getParameters();
            int paramsSize = params.size();
            int[] result = new int[size];
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < paramsSize; j++) {
                    CommandParameter p = params.get(j);
                    p.setValue(in.readValue());
                }
                try {
                    result[i] = command.executeUpdate();
                } catch (Exception e) {
                    result[i] = Statement.EXECUTE_FAILED;
                }
            }
            TransferOutputStream out = createTransferOutputStream(session);
            writeBatchResult(out, session, packetId, result);
            break;
        }
        case Session.COMMAND_CLOSE: {
            int commandId = in.readInt();
            PreparedSQLStatement command = (PreparedSQLStatement) cache.remove(commandId, true);
            if (command != null) {
                command.close();
            }
            break;
        }
        case Session.RESULT_FETCH_ROWS: {
            int resultId = in.readInt();
            int count = in.readInt();
            Result result = (Result) cache.get(resultId);
            TransferOutputStream out = createTransferOutputStream(session);
            out.writeResponseHeader(packetId, Session.STATUS_OK);
            writeRow(out, result, count);
            out.flush();
            break;
        }
        case Session.RESULT_RESET: {
            int resultId = in.readInt();
            Result result = (Result) cache.get(resultId);
            result.reset();
            break;
        }
        case Session.RESULT_CHANGE_ID: {
            int oldId = in.readInt();
            int newId = in.readInt();
            AutoCloseable obj = cache.remove(oldId, false);
            cache.put(newId, obj);
            break;
        }
        case Session.RESULT_CLOSE: {
            int resultId = in.readInt();
            Result result = (Result) cache.remove(resultId, true);
            if (result != null) {
                result.close();
            }
            break;
        }
        case Session.SESSION_SET_AUTO_COMMIT: {
            boolean autoCommit = in.readBoolean();
            session.setAutoCommit(autoCommit);
            TransferOutputStream out = createTransferOutputStream(session);
            out.writeResponseHeader(packetId, Session.STATUS_OK).flush();
            break;
        }
        case Session.SESSION_CLOSE: {
            closeSession(packetId, sessionId);
            break;
        }
        case Session.SESSION_CANCEL_STATEMENT: {
            int statementId = in.readInt();
            PreparedSQLStatement command = (PreparedSQLStatement) cache.remove(statementId, false);
            if (command != null) {
                command.cancel();
                command.close();
            } else {
                session.cancelStatement(statementId);
            }
            break;
        }
        case Session.COMMAND_READ_LOB: {
            if (lobs == null) {
                lobs = SmallLRUCache.newInstance(
                        Math.max(SysProperties.SERVER_CACHED_OBJECTS, SysProperties.SERVER_RESULT_SET_FETCH_SIZE * 5));
            }
            long lobId = in.readLong();
            byte[] hmac = in.readBytes();
            CachedInputStream cachedInputStream = lobs.get(lobId);
            if (cachedInputStream == null) {
                cachedInputStream = new CachedInputStream(null);
                lobs.put(lobId, cachedInputStream);
            }
            long offset = in.readLong();
            int length = in.readInt();
            TransferOutputStream out = createTransferOutputStream(session);
            out.verifyLobMac(hmac, lobId);
            if (cachedInputStream.getPos() != offset) {
                LobStorage lobStorage = session.getDataHandler().getLobStorage();
                // only the lob id is used
                ValueLob lob = ValueLob.create(Value.BLOB, null, -1, lobId, hmac, -1);
                InputStream lobIn = lobStorage.getInputStream(lob, hmac, -1);
                cachedInputStream = new CachedInputStream(lobIn);
                lobs.put(lobId, cachedInputStream);
                lobIn.skip(offset);
            }
            // limit the buffer size
            length = Math.min(16 * Constants.IO_BUFFER_SIZE, length);
            byte[] buff = new byte[length];
            length = IOUtils.readFully(cachedInputStream, buff, length);
            out.writeResponseHeader(packetId, Session.STATUS_OK);
            out.writeInt(length);
            out.writeBytes(buff, 0, length);
            out.flush();
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

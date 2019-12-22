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

import java.io.IOException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.ExpiringMap;
import org.lealone.common.util.Pair;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.db.CommandParameter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.ServerSession;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.Result;
import org.lealone.net.TransferConnection;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.net.WritableChannel;
import org.lealone.server.Scheduler.PreparedCommand;
import org.lealone.server.Scheduler.SessionInfo;
import org.lealone.server.handler.CachedInputStream;
import org.lealone.server.handler.PacketHandler;
import org.lealone.server.handler.PacketHandlers;
import org.lealone.server.protocol.InitPacket;
import org.lealone.server.protocol.InitPacketAck;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketDecoders;
import org.lealone.server.protocol.result.ResultFetchRowsAck;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.storage.PageKey;

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

    void readInitPacketV2(TransferInputStream in, int packetId, int sessionId) {
        try {
            InitPacket packet = InitPacket.decoder.decode(in, 0);
            ConnectionInfo ci = packet.ci;
            String baseDir = tcpServer.getBaseDir();
            if (baseDir == null) {
                baseDir = SysProperties.getBaseDirSilently();
            }
            // 强制使用服务器端的基目录
            if (baseDir != null) {
                ci.setBaseDir(baseDir);
            }
            Session session = createSession(ci, sessionId);
            session.setProtocolVersion(packet.clientVersion);
            in.setSession(session);

            TransferOutputStream out = createTransferOutputStream(session);
            out.writeResponseHeader(packetId, Session.STATUS_OK);
            InitPacketAck ack = new InitPacketAck(packet.clientVersion, session.isAutoCommit(),
                    session.getTargetNodes(), session.getRunMode(), session.isInvalid());
            ack.encode(out, packet.clientVersion);
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

    protected void executeQueryAsync(TransferInputStream in, int packetId, int packetType, SessionInfo si,
            boolean prepared) throws IOException {
        final Session session = si.session;
        final int sessionId = si.sessionId;

        int resultId = in.readInt();
        int maxRows = in.readInt();
        int fetchSize = in.readInt();
        boolean scrollable = in.readBoolean();

        if (packetType == Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY
                || packetType == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY) {
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
        if (executeQueryAsync(packetId, packetType, session, sessionId, stmt, resultId, fetchSize)) {
            return;
        }
        PreparedSQLStatement.Yieldable<?> yieldable = stmt.createYieldableQuery(maxRows, scrollable, ar -> {
            if (ar.isSucceeded()) {
                Result result = ar.getResult();
                sendResult(packetId, packetType, session, sessionId, result, resultId, fetchSize);
            } else {
                sendError(session, packetId, ar.getCause());
            }
        });
        yieldable.setPageKeys(pageKeys);
        addPreparedCommandToQueue(packetId, si, stmt, yieldable);
    }

    protected boolean executeQueryAsync(int packetId, int packetType, Session session, int sessionId,
            PreparedSQLStatement stmt, int resultId, int fetchSize) throws IOException {
        return false;
    }

    protected void sendResult(int packetId, int packetType, Session session, int sessionId, Result result, int resultId,
            int fetchSize) {
        cache.put(resultId, result);
        try {
            TransferOutputStream out = createTransferOutputStream(session);
            out.writeResponseHeader(packetId, getStatus(session));
            if (packetType == Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY
                    || packetType == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY) {
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
            ResultFetchRowsAck.writeRow(out, result, fetch);
            out.flush();
        } catch (Exception e) {
            sendError(session, packetId, e);
        }
    }

    protected void executeUpdateAsync(TransferInputStream in, int packetId, int packetType, SessionInfo si,
            boolean prepared) throws IOException {
        final Session session = si.session;
        final int sessionId = si.sessionId;

        if (packetType == Session.COMMAND_REPLICATION_UPDATE
                || packetType == Session.COMMAND_REPLICATION_PREPARED_UPDATE) {
            session.setReplicationName(in.readString());
        } else if (packetType == Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE
                || packetType == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE) {
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
                    if (packetType == Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE
                            || packetType == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE) {
                        out.writeString(session.getTransaction().getLocalTransactionNames());
                    }
                    out.writeInt(updateCount);
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
    protected void handleRequest(TransferInputStream in, int packetId, int packetType) throws IOException {
        // 这里的sessionId是客户端session的id，每个数据包都会带这个字段
        int sessionId = in.readInt();
        SessionInfo si = getSessionInfo(sessionId);
        if (si == null) {
            // 创建新session时临时分配一个调度器，当新session创建成功后再分配一个固定的调度器，
            // 之后此session相关的请求包和命令都由固定的调度器负责处理。
            if (packetType == Session.SESSION_INIT) {
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
                    handleRequest(in, packetId, packetType, si);
                } catch (Throwable e) {
                    logger.error("Failed to handle request, packetId: " + packetId + ", packetType: " + packetType, e);
                    sendError(si.session, packetId, e);
                } finally {
                    // in.closeInputStream(); // 到这里输入流已经读完，及时释放NetBuffer
                }
            });
        }
    }

    // 这个方法就已经是由调度器线程执行了
    private void handleRequest(TransferInputStream in, int packetId, int packetType, SessionInfo si)
            throws IOException {
        Session session = si.session;
        int sessionId = si.sessionId;
        switch (packetType) {
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
            if (packetType == Session.COMMAND_PREPARE_READ_PARAMS) {
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
            executeQueryAsync(in, packetId, packetType, si, false);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY:
        case Session.COMMAND_PREPARED_QUERY: {
            executeQueryAsync(in, packetId, packetType, si, true);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE:
        case Session.COMMAND_UPDATE:
        case Session.COMMAND_REPLICATION_UPDATE: {
            executeUpdateAsync(in, packetId, packetType, si, false);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE:
        case Session.COMMAND_PREPARED_UPDATE:
        case Session.COMMAND_REPLICATION_PREPARED_UPDATE: {
            executeUpdateAsync(in, packetId, packetType, si, true);
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
        default:
            handleOtherRequest(in, packetId, packetType, si);
        }
    }

    private void handleOtherRequest(TransferInputStream in, int packetId, int packetType, SessionInfo si)
            throws IOException {
        Session session = si.session;
        int version = session.getProtocolVersion();
        PacketDecoder<? extends Packet> decoder = PacketDecoders.getDecoder(packetType);
        Packet packet = decoder.decode(in, version);
        @SuppressWarnings("unchecked")
        PacketHandler<Packet> handler = (PacketHandler<Packet>) PacketHandlers.getHandler(packetType);
        if (handler != null) {
            Packet ack = handler.handle(this, (ServerSession) session, packet);
            if (ack != null) {
                TransferOutputStream out = createTransferOutputStream(session);
                writeResponseHeader(out, session, packetId);
                ack.encode(out, version);
                out.flush();
            }
        } else {
            logger.warn("Unknow packet type: {}", packetType);
            close();
        }
    }

    public void addCache(Integer k, AutoCloseable v) {
        cache.put(k, v);
    }

    public AutoCloseable getCache(Integer k) {
        return cache.get(k);
    }

    public AutoCloseable removeCache(Integer k, boolean ifAvailable) {
        return cache.remove(k, ifAvailable);
    }

    public SmallLRUCache<Long, CachedInputStream> getLobs() {
        if (lobs == null) {
            lobs = SmallLRUCache.newInstance(
                    Math.max(SysProperties.SERVER_CACHED_OBJECTS, SysProperties.SERVER_RESULT_SET_FETCH_SIZE * 5));
        }
        return lobs;
    }
}

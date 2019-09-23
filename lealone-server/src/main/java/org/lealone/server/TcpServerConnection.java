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
import org.lealone.common.util.StringUtils;
import org.lealone.db.CommandParameter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.DataBuffer;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncTaskHandler;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLob;
import org.lealone.db.value.ValueLong;
import org.lealone.net.TcpConnection;
import org.lealone.net.Transfer;
import org.lealone.net.WritableChannel;
import org.lealone.server.Scheduler.CommandQueue;
import org.lealone.server.Scheduler.PreparedCommand;
import org.lealone.sql.PreparedStatement;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.LobStorage;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

/**
 * An async tcp server connection.
 * 
 * @author H2 Group
 * @author zhh
 */
public class TcpServerConnection extends TcpConnection {

    private static final Logger logger = LoggerFactory.getLogger(TcpServerConnection.class);

    // 每个sessionId对应一个专有的CommandQueue，所有与这个sessionId相关的命令请求都先放到这个队列，
    // 然后由调度器根据优先级从多个CommandQueue中依次取出执行
    private final ConcurrentHashMap<Integer, CommandQueue> commandQueueMap = new ConcurrentHashMap<>();
    private final SmallMap cache = new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private final AsyncTaskHandler asyncTaskHandler;
    private SmallLRUCache<Long, CachedInputStream> lobs; // 大多数情况下都不使用lob，所以延迟初始化
    private String baseDir;

    public TcpServerConnection(WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
        asyncTaskHandler = ScheduleService.getScheduler();
    }

    void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    protected SmallMap getCache() {
        return cache;
    }

    @Override
    public AsyncTaskHandler getAsyncTaskHandler() {
        return asyncTaskHandler;
    }

    @Override
    public void close() {
        super.close();
        for (CommandQueue queue : commandQueueMap.values()) {
            queue.scheduler.removeCommandQueue(queue);
        }
        commandQueueMap.clear();
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
            String dbName = transfer.readString();
            String originalURL = transfer.readString();
            String userName = transfer.readString();
            userName = StringUtils.toUpperEnglish(userName);
            Session session = createSession(transfer, sessionId, originalURL, dbName, userName);
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

    private Session createSession(Transfer transfer, int sessionId, String originalURL, String dbName, String userName)
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
        Session session = ci.createSession();
        if (session.isValid()) {
            addSession(sessionId, session);
            assignScheduler(sessionId);
        }
        return session;
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

    protected void executeQueryAsync(Transfer transfer, Session session, int sessionId, int id, int operation,
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

        PreparedStatement command;
        if (prepared) {
            command = (PreparedStatement) cache.getObject(id, false);
            setParameters(transfer, command);
        } else {
            String sql = transfer.readString();
            command = session.prepareStatement(sql, fetchSize);
            cache.addObject(id, command);
        }
        command.setFetchSize(fetchSize);

        List<PageKey> pageKeys = readPageKeys(transfer);
        if (executeQueryAsync(session, sessionId, command, transfer, id, operation, resultId, fetchSize)) {
            return;
        }
        PreparedStatement.Yieldable<?> yieldable = command.createYieldableQuery(maxRows, scrollable, pageKeys, ar -> {
            if (ar.isSucceeded()) {
                Result result = ar.getResult();
                sendResult(transfer, session, sessionId, id, operation, result, resultId, fetchSize);
            } else {
                sendError(transfer, id, ar.getCause());
            }
        });

        addPreparedCommandToQueue(id, command, transfer, session, sessionId, yieldable);
    }

    protected boolean executeQueryAsync(Session session, int sessionId, PreparedStatement command, Transfer transfer,
            int id, int operation, int resultId, int fetchSize) throws IOException {
        return false;
    }

    protected void sendResult(Transfer transfer, Session session, int sessionId, int id, int operation, Result result,
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

    protected void executeUpdateAsync(Transfer transfer, Session session, int sessionId, int id, int operation,
            boolean prepared) throws IOException {
        if (operation == Session.COMMAND_REPLICATION_UPDATE
                || operation == Session.COMMAND_REPLICATION_PREPARED_UPDATE) {
            session.setReplicationName(transfer.readString());
        } else if (operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE
                || operation == Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE) {
            session.setAutoCommit(false);
            session.setRoot(false);
        }

        PreparedStatement command;
        if (prepared) {
            command = (PreparedStatement) cache.getObject(id, false);
            setParameters(transfer, command);
        } else {
            String sql = transfer.readString();
            command = session.prepareStatement(sql, -1);
            cache.addObject(id, command);
        }

        List<PageKey> pageKeys = readPageKeys(transfer);

        PreparedStatement.Yieldable<?> yieldable = command.createYieldableUpdate(pageKeys, ar -> {
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

        addPreparedCommandToQueue(id, command, transfer, session, sessionId, yieldable);
    }

    private void addPreparedCommandToQueue(int id, PreparedStatement stmt, Transfer transfer, Session session,
            int sessionId, PreparedStatement.Yieldable<?> yieldable) {
        CommandQueue queue = commandQueueMap.get(sessionId);
        if (queue == null) {
            commandQueueNotFound(sessionId);
        }
        PreparedCommand pc = new PreparedCommand(id, stmt, transfer, session, yieldable, queue);

        // asyncTaskHandler是执行当前方法的线程，如果即将被执行的命令也被分配到同样的线程中(scheduler)运行，
        // 那么就不需要放到队列中了直接执行即可。
        if (queue.scheduler == asyncTaskHandler) {
            pc.execute();
        } else {
            queue.addCommand(pc);
        }
    }

    private void commandQueueNotFound(int sessionId) {
        String msg = "CommandQueue is null, may be a bug! sessionId = " + sessionId;
        logger.warn(msg);
        throw DbException.throwInternalError(msg);
    }

    // 每个sessionId对应一个CommandQueue，
    // 每个调度器可以负责多个CommandQueue，
    // 但是一个CommandQueue只能由一个调度器负责。
    private void assignScheduler(int sessionId) {
        Scheduler scheduler = ScheduleService.getScheduler();
        CommandQueue queue = new CommandQueue(scheduler);
        commandQueueMap.put(sessionId, queue);
        scheduler.addCommandQueue(queue);
    }

    @Override
    protected void handleRequest(Transfer transfer, int id, int operation) throws IOException {
        int sessionId = transfer.readInt();
        Session session = getSession(sessionId);
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
            executeQueryAsync(transfer, session, sessionId, id, operation, false);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY:
        case Session.COMMAND_PREPARED_QUERY: {
            executeQueryAsync(transfer, session, sessionId, id, operation, true);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE:
        case Session.COMMAND_UPDATE:
        case Session.COMMAND_REPLICATION_UPDATE: {
            executeUpdateAsync(transfer, session, sessionId, id, operation, false);
            break;
        }
        case Session.COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE:
        case Session.COMMAND_PREPARED_UPDATE:
        case Session.COMMAND_REPLICATION_PREPARED_UPDATE: {
            executeUpdateAsync(transfer, session, sessionId, id, operation, true);
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
            PageKey pageKey = transfer.readPageKey();
            ByteBuffer page = transfer.readByteBuffer();
            boolean addPage = transfer.readBoolean();
            StorageMap<Object, Object> map = session.getStorageMap(mapName);
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
            StorageMap<Object, Object> map = session.getStorageMap(mapName);
            ByteBuffer page = map.readPage(pageKey);
            writeResponseHeader(transfer, session, id);
            transfer.writeByteBuffer(page);
            transfer.flush();
            break;
        }
        case Session.COMMAND_STORAGE_REMOVE_LEAF_PAGE: {
            String mapName = transfer.readString();
            PageKey pageKey = transfer.readPageKey();

            StorageMap<Object, Object> map = session.getStorageMap(mapName);
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
            CommandQueue queue = commandQueueMap.remove(sessionId);
            if (queue != null) {
                queue.scheduler.removeCommandQueue(queue);
                session = removeSession(sessionId);
                closeSession(session);
            } else {
                commandQueueNotFound(sessionId);
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
}

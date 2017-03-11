/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.exceptions.LealoneException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceSystem;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.TempFileDeleter;
import org.lealone.db.Command;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.DataHandler;
import org.lealone.db.RunMode;
import org.lealone.db.Session;
import org.lealone.db.SessionBase;
import org.lealone.db.SetTypes;
import org.lealone.db.SysProperties;
import org.lealone.db.value.Value;
import org.lealone.net.AsyncCallback;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetFactory;
import org.lealone.net.Transfer;
import org.lealone.replication.ReplicationSession;
import org.lealone.sql.ParsedStatement;
import org.lealone.sql.PreparedStatement;
import org.lealone.storage.LobStorage;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;
import org.lealone.transaction.Transaction;

import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;

/**
 * The client side part of a session when using the server mode. 
 * This object communicates with a session on the server side.
 * 
 * @author H2 Group
 * @author zhh
 */
public class ClientSession extends SessionBase implements DataHandler, Transaction.Participant {

    private static final ConcurrentHashMap<String, AsyncConnection> asyncConnections = new ConcurrentHashMap<>();

    private static Vertx vertx;
    private static NetClient client;

    private TraceSystem traceSystem;
    private Trace trace;
    private Transfer transfer;
    private boolean autoCommit = true;
    private final ConnectionInfo ci;
    private String cipher;
    private byte[] fileEncryptionKey;
    private final Object lobSyncObject = new Object();
    private int sessionId;
    private LobStorage lobStorage;
    private Transaction transaction;
    private AsyncConnection asyncConnection;

    public ClientSession(ConnectionInfo ci) {
        this.ci = ci;
        if (vertx == null) {
            synchronized (ClientSession.class) {
                if (vertx == null) {
                    vertx = NetFactory.getVertx(ci.getProperties());
                    NetClientOptions options = NetFactory.getNetClientOptions(ci.getProperties());
                    options.setConnectTimeout(10000);
                    client = vertx.createNetClient(options);
                }
            }
        }
    }

    public int getSessionId() {
        return sessionId;
    }

    /**
     * Open a new (remote or embedded) session.
     *
     * @return the session
     */
    @Override
    public Session connectEmbeddedOrServer() {
        return connectEmbeddedOrServer(true);
    }

    @Override
    public Session connectEmbeddedOrServer(boolean first) {
        if (ci.isRemote()) {
            connectServer();
            if (first) {
                if (getRunMode() == RunMode.REPLICATION) {
                    ConnectionInfo ci = this.ci;
                    String[] servers = StringUtils.arraySplit(getTargetEndpoints(), ',', true);
                    int size = servers.length;
                    Session[] sessions = new ClientSession[size];
                    for (int i = 0; i < size; i++) {
                        ci = this.ci.copy(servers[i]);
                        sessions[i] = new ClientSession(ci);
                        sessions[i] = sessions[i].connectEmbeddedOrServer(false);
                    }
                    return new ReplicationSession(sessions);
                }
                if (isInvalid()) {
                    switch (getRunMode()) {
                    case CLIENT_SERVER:
                    case SHARDING: {
                        ConnectionInfo ci = this.ci.copy(getTargetEndpoints());
                        ClientSession session = new ClientSession(ci);
                        return session.connectEmbeddedOrServer(false);
                    }
                    default:
                        return this;
                    }
                }
            }
            return this;
        } else if (ci.isReplicaSetMode()) {
            ConnectionInfo ci = this.ci;
            String[] servers = StringUtils.arraySplit(ci.getServers(), ',', true);
            int size = servers.length;
            Session[] sessions = new ClientSession[size];
            for (int i = 0; i < size; i++) {
                ci = this.ci.copy(servers[i]);
                sessions[i] = new ClientSession(ci);
                sessions[i] = sessions[i].connectEmbeddedOrServer(false);
            }
            return new ReplicationSession(sessions);
        }
        try {
            return ci.getSessionFactory().createSession(ci);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private void connectServer() {
        initTraceSystem();

        cipher = ci.getProperty("CIPHER");
        if (cipher != null) {
            fileEncryptionKey = MathUtils.secureRandomBytes(32);
        }

        transfer = null;
        String[] servers = StringUtils.arraySplit(ci.getServers(), ',', true);
        Random random = new Random(System.currentTimeMillis());
        try {
            for (int i = 0, len = servers.length; i < len; i++) {
                String s = servers[random.nextInt(len)];
                try {
                    transfer = initTransfer(ci, s);
                    break;
                } catch (Exception e) {
                    if (i == len - 1) {
                        throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, e, e + ": " + s);
                    }
                    int index = 0;
                    String[] newServers = new String[len - 1];
                    for (int j = 0; j < len; j++) {
                        if (j != i)
                            newServers[index++] = servers[j];
                    }
                    servers = newServers;
                    len--;
                    i = -1;
                }
            }
        } catch (DbException e) {
            traceSystem.close();
            throw e;
        }
    }

    @Override
    public int getNextId() {
        if (asyncConnection == null)
            super.getNextId();
        return asyncConnection.getNextId();
    }

    private void initTraceSystem() {
        traceSystem = new TraceSystem();
        String traceLevelFile = ci.getProperty(SetTypes.TRACE_LEVEL_FILE, null);
        if (traceLevelFile != null) {
            int level = Integer.parseInt(traceLevelFile);
            String prefix = getFilePrefix(SysProperties.CLIENT_TRACE_DIRECTORY, ci.getDatabaseName());
            try {
                traceSystem.setLevelFile(level);
                if (level > 0) {
                    String file = FileUtils.createTempFile(prefix, Constants.SUFFIX_TRACE_FILE, false, false);
                    traceSystem.setFileName(file);
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, prefix);
            }
        }
        String traceLevelSystemOut = ci.getProperty(SetTypes.TRACE_LEVEL_SYSTEM_OUT, null);
        if (traceLevelSystemOut != null) {
            int level = Integer.parseInt(traceLevelSystemOut);
            traceSystem.setLevelSystemOut(level);
        }
        trace = traceSystem.getTrace(Trace.JDBC);
    }

    private Transfer initTransfer(ConnectionInfo ci, String server) throws Exception {
        int port = Constants.DEFAULT_TCP_PORT;
        // IPv6: RFC 2732 format is '[a:b:c:d:e:f:g:h]' or
        // '[a:b:c:d:e:f:g:h]:port'
        // RFC 2396 format is 'a.b.c.d' or 'a.b.c.d:port' or 'hostname' or
        // 'hostname:port'
        int startIndex = server.startsWith("[") ? server.indexOf(']') : 0;
        int idx = server.indexOf(':', startIndex);
        if (idx >= 0) {
            port = Integer.decode(server.substring(idx + 1));
            server = server.substring(0, idx);
        }

        final String hostAndPort = server + ":" + port;

        asyncConnection = asyncConnections.get(hostAndPort);
        if (asyncConnection == null) {
            synchronized (ClientSession.class) {
                asyncConnection = asyncConnections.get(hostAndPort);
                if (asyncConnection == null) {
                    CountDownLatch latch = new CountDownLatch(1);
                    client.connect(port, server, res -> {
                        if (res.succeeded()) {
                            NetSocket socket = res.result();
                            asyncConnection = new AsyncConnection(socket, false);
                            asyncConnection.setHostAndPort(hostAndPort);
                            asyncConnections.put(hostAndPort, asyncConnection);
                            socket.handler(asyncConnection);
                            latch.countDown();
                        } else {
                            throw DbException.convert(res.cause());
                        }
                    });
                    latch.await();
                }
            }
        }
        sessionId = getNextId();
        transfer = asyncConnection.createTransfer(this);
        asyncConnection.writeInitPacket(this, sessionId, transfer, ci);
        asyncConnection.addSession(sessionId, this);
        return transfer;
    }

    private static String getFilePrefix(String dir, String dbName) {
        StringBuilder buff = new StringBuilder(dir);
        if (!(dir.charAt(dir.length() - 1) == File.separatorChar))
            buff.append(File.separatorChar);
        for (int i = 0, length = dbName.length(); i < length; i++) {
            char ch = dbName.charAt(i);
            if (Character.isLetterOrDigit(ch)) {
                buff.append(ch);
            } else {
                buff.append('_');
            }
        }
        return buff.toString();
    }

    @Override
    public void cancel() {
        // this method is called when closing the connection
        // the statement that is currently running is not canceled in this case
        // however Statement.cancel is supported
    }

    /**
     * Cancel the statement with the given id.
     *
     * @param id the statement id
     */
    public void cancelStatement(int id) {
        try {
            transfer.writeRequestHeader(id, Session.SESSION_CANCEL_STATEMENT).flush();
        } catch (IOException e) {
            trace.debug(e, "could not cancel statement");
        }
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        if (this.autoCommit == autoCommit)
            return;
        setAutoCommitSend(autoCommit);
        this.autoCommit = autoCommit;
    }

    private void setAutoCommitSend(boolean autoCommit) {
        try {
            int id = getNextId();
            traceOperation("SESSION_SET_AUTOCOMMIT", autoCommit ? 1 : 0);
            transfer.writeRequestHeader(id, Session.SESSION_SET_AUTO_COMMIT);
            transfer.writeInt(sessionId).writeBoolean(autoCommit);
            AsyncCallback<Void> ac = new AsyncCallback<>();
            transfer.addAsyncCallback(id, ac);
            transfer.flush();
            ac.await();
        } catch (IOException e) {
            handleException(e);
        }
    }

    public void handleException(Exception e) {
        checkClosed();
        throw new LealoneException(e);
    }

    @Override
    public Command createCommand(String sql, int fetchSize) {
        checkClosed();
        return new ClientCommand(this, transfer.copy(this), sql, fetchSize);
    }

    @Override
    public StorageCommand createStorageCommand() {
        checkClosed();
        return new ClientCommand(this, transfer.copy(this), null, -1);
    }

    @Override
    public Command prepareCommand(String sql, int fetchSize) {
        Command c = createCommand(sql, fetchSize);
        c.prepare();
        return c;
    }

    /**
     * Check if this session is closed and throws an exception if so.
     *
     * @throws DbException if the session is closed
     */
    public void checkClosed() {
        if (isClosed()) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "session closed");
        }
    }

    @Override
    public boolean isClosed() {
        return transfer == null;
    }

    @Override
    public void close() {
        RuntimeException closeError = null;
        synchronized (this) {
            try {
                traceOperation("SESSION_CLOSE", 0);
                transfer.writeRequestHeader(sessionId, Session.SESSION_CLOSE).flush();
                asyncConnection.removeSession(sessionId);

                synchronized (ClientSession.class) {
                    if (asyncConnection.isEmpty()) {
                        asyncConnections.remove(asyncConnection.getHostAndPort());
                    }
                    if (asyncConnections.isEmpty()) {
                        client.close();
                        vertx.close();
                        client = null;
                        vertx = null;
                    }
                }
            } catch (RuntimeException e) {
                trace.error(e, "close");
                closeError = e;
            } catch (Exception e) {
                trace.error(e, "close");
            }

        }
        transfer = null;
        traceSystem.close();
        if (closeError != null) {
            throw closeError;
        }
    }

    @Override
    public Trace getTrace() {
        return traceSystem.getTrace(Trace.JDBC);
    }

    /**
     * Write the operation to the trace system if debug trace is enabled.
     *
     * @param operation the operation performed
     * @param id the id of the operation
     */
    public void traceOperation(String operation, int id) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} {1}", operation, id);
        }
    }

    @Override
    public void checkPowerOff() {
        // ok
    }

    @Override
    public void checkWritingAllowed() {
        // ok
    }

    @Override
    public String getDatabasePath() {
        return "";
    }

    @Override
    public String getLobCompressionAlgorithm(int type) {
        return null;
    }

    @Override
    public int getMaxLengthInplaceLob() {
        return SysProperties.LOB_CLIENT_MAX_SIZE_MEMORY;
    }

    @Override
    public FileStorage openFile(String name, String mode, boolean mustExist) {
        if (mustExist && !FileUtils.exists(name)) {
            throw DbException.get(ErrorCode.FILE_NOT_FOUND_1, name);
        }
        FileStorage fileStorage;
        if (cipher == null) {
            fileStorage = FileStorage.open(this, name, mode);
        } else {
            fileStorage = FileStorage.open(this, name, mode, cipher, fileEncryptionKey, 0);
        }
        fileStorage.setCheckedWriting(false);
        try {
            fileStorage.init();
        } catch (DbException e) {
            fileStorage.closeSilently();
            throw e;
        }
        return fileStorage;
    }

    @Override
    public DataHandler getDataHandler() {
        return this;
    }

    @Override
    public Object getLobSyncObject() {
        return lobSyncObject;
    }

    @Override
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return null;
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        return TempFileDeleter.getInstance();
    }

    @Override
    public LobStorage getLobStorage() {
        if (lobStorage == null) {
            lobStorage = new ClientLobStorage(this);
        }
        return lobStorage;
    }

    @Override
    public Connection getLobConnection() {
        return null;
    }

    @Override
    public synchronized int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off, int length) {
        try {
            int id = getNextId();
            traceOperation("LOB_READ", (int) lobId);
            transfer.writeRequestHeader(id, Session.COMMAND_READ_LOB);
            transfer.writeInt(sessionId);
            transfer.writeLong(lobId);
            transfer.writeBytes(hmac);
            transfer.writeLong(offset);
            transfer.writeInt(length);
            transfer.flush();
            length = transfer.readInt();
            if (length <= 0) {
                return length;
            }
            transfer.readBytes(buff, off, length);
            return length;
        } catch (IOException e) {
            handleException(e);
        }
        return 1;
    }

    @Override
    public synchronized void commitTransaction(String allLocalTransactionNames) {
        checkClosed();
        try {
            int id = getNextId();
            transfer.writeRequestHeader(id, Session.COMMAND_DISTRIBUTED_TRANSACTION_COMMIT);
            transfer.writeInt(sessionId).writeString(allLocalTransactionNames).flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void rollbackTransaction() {
        checkClosed();
        try {
            int id = getNextId();
            transfer.writeRequestHeader(id, Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK);
            transfer.writeInt(sessionId).flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void addSavepoint(String name) {
        checkClosed();
        try {
            int id = getNextId();
            transfer.writeRequestHeader(id, Session.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT);
            transfer.writeInt(sessionId).writeString(name);
            transfer.flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void rollbackToSavepoint(String name) {
        checkClosed();
        try {
            int id = getNextId();
            transfer.writeRequestHeader(id, Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT);
            transfer.writeInt(sessionId).writeString(name);
            transfer.flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized boolean validateTransaction(String localTransactionName) {
        checkClosed();
        try {
            int id = getNextId();
            transfer.writeRequestHeader(id, Session.COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE);
            transfer.writeInt(sessionId).writeString(localTransactionName).flush();
            return transfer.readBoolean();
        } catch (Exception e) {
            handleException(e);
            return false;
        }
    }

    // 要加synchronized，避免ClientCommand在执行更新和查询时其他线程把transaction置null
    @Override
    public synchronized void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public Transaction getTransaction() {
        return transaction;
    }

    public synchronized ClientBatchCommand getClientBatchCommand(ArrayList<String> batchCommands) {
        checkClosed();
        return new ClientBatchCommand(this, transfer.copy(this), batchCommands);
    }

    public synchronized ClientBatchCommand getClientBatchCommand(Command preparedCommand,
            ArrayList<Value[]> batchParameters) {
        checkClosed();
        return new ClientBatchCommand(this, transfer.copy(this), preparedCommand, batchParameters);
    }

    @Override
    public String getURL() {
        return ci.getURL();
    }

    @Override
    public ParsedStatement parseStatement(String sql) {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int fetchSize) {
        return null;
    }

    @Override
    public int getModificationId() {
        return 0;
    }

    @Override
    public void rollback() {
    }

    @Override
    public void setRoot(boolean isRoot) {
    }

    @Override
    public void commit(String allLocalTransactionNames) {
    }

    @Override
    public ConnectionInfo getConnectionInfo() {
        return ci;
    }

}

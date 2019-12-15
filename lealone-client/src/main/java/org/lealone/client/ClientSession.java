/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.sql.Connection;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.exceptions.LealoneException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.common.util.TempFileDeleter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.DataHandler;
import org.lealone.db.Session;
import org.lealone.db.SessionBase;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.net.AsyncCallback;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.NetNode;
import org.lealone.net.TcpClientConnection;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.sql.ParsedSQLStatement;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.LobStorage;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.replication.ReplicaSQLCommand;
import org.lealone.storage.replication.ReplicaStorageCommand;
import org.lealone.transaction.Transaction;

/**
 * The client side part of a session when using the server mode. 
 * This object communicates with a session on the server side.
 * 
 * @author H2 Group
 * @author zhh
 */
// 一个ClientSession对应一条JdbcConnection，
// 同JdbcConnection一样，每个ClientSession对象也不是线程安全的，只能在单线程中使用。
// 另外，每个ClientSession只对应一个server，
// 虽然ConnectionInfo允许在JDBC URL中指定多个server，但是放在AutoReconnectSession中处理了。
public class ClientSession extends SessionBase implements DataHandler, Transaction.Participant {

    private final ConnectionInfo ci;
    private final String server;
    private final Session parent;
    private final String cipher;
    private final byte[] fileEncryptionKey;
    private final Trace trace;
    private final Object lobSyncObject = new Object();
    private LobStorage lobStorage;

    private TcpClientConnection tcpConnection;
    private int sessionId;

    ClientSession(ConnectionInfo ci, String server, Session parent) {
        if (!ci.isRemote()) {
            throw DbException.throwInternalError();
        }
        this.ci = ci;
        this.server = server;
        this.parent = parent;

        cipher = ci.getProperty("CIPHER");
        fileEncryptionKey = cipher == null ? null : MathUtils.secureRandomBytes(32);

        initTraceSystem(ci);
        trace = traceSystem == null ? Trace.NO_TRACE : traceSystem.getTrace(TraceModuleType.JDBC);
    }

    @Override
    public int getSessionId() {
        return sessionId;
    }

    @Override
    public int getNextId() {
        if (tcpConnection == null) {
            if (sessionId <= 0) {
                open();
            } else {
                checkClosed();
            }
        }
        return tcpConnection.getNextId();
    }

    @Override
    public int getCurrentId() {
        if (tcpConnection == null) {
            return 0;
        }
        return tcpConnection.getCurrentId();
    }

    @Override
    public Session connect(boolean allowRedirect) {
        open();
        return this;
    }

    // 在AutoReconnectSession类中有单独使用，所以用包访问级别
    TcpClientConnection open() {
        if (tcpConnection != null)
            return tcpConnection;

        try {
            NetNode node = NetNode.createTCP(server);
            NetFactory factory = NetFactoryManager.getFactory(ci.getNetFactoryName());
            CaseInsensitiveMap<String> config = new CaseInsensitiveMap<>(ci.getProperties());
            // 多个客户端session会共用同一条TCP连接
            AsyncConnection conn = factory.getNetClient().createConnection(config, node);
            if (!(conn instanceof TcpClientConnection)) {
                throw DbException.throwInternalError("not tcp client connection: " + conn.getClass().getName());
            }
            tcpConnection = (TcpClientConnection) conn;
            // 每一个通过网络传输的协议包都会带上sessionId，
            // 这样就能在同一条TCP连接中区分不同的客户端session了
            sessionId = tcpConnection.getNextId();
            tcpConnection.writeInitPacket(this);
            if (isValid()) {
                tcpConnection.addSession(sessionId, this);
            }
        } catch (Throwable e) {
            closeTraceSystem();
            throw DbException.convert(e);
        }
        return tcpConnection;
    }

    public TransferOutputStream newOut() {
        checkClosed();
        return tcpConnection.createTransferOutputStream(this);
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
     * @param statementId the statement id
     */
    @Override
    public void cancelStatement(int statementId) {
        try {
            newOut().writeRequestHeader(Session.SESSION_CANCEL_STATEMENT).writeInt(statementId).flush();
        } catch (IOException e) {
            trace.debug(e, "could not cancel statement");
        }
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
            TransferOutputStream out = newOut();
            int packetId = getNextId();
            traceOperation("SESSION_SET_AUTOCOMMIT", autoCommit ? 1 : 0);
            out.writeRequestHeader(packetId, Session.SESSION_SET_AUTO_COMMIT);
            out.writeBoolean(autoCommit);
            out.flushAndAwait(packetId);
        } catch (IOException e) {
            handleException(e);
        }
    }

    public void handleException(Exception e) {
        checkClosed();
        if (e instanceof DbException)
            throw (DbException) e;
        throw new LealoneException(e);
    }

    @Override
    public SQLCommand createSQLCommand(String sql, int fetchSize) {
        checkClosed();
        return new ClientSQLCommand(this, sql, fetchSize);
    }

    @Override
    public ReplicaSQLCommand createReplicaSQLCommand(String sql, int fetchSize) {
        checkClosed();
        return new ClientSQLCommand(this, sql, fetchSize);
    }

    @Override
    public StorageCommand createStorageCommand() {
        checkClosed();
        return new ClientStorageCommand(this);
    }

    @Override
    public ReplicaStorageCommand createReplicaStorageCommand() {
        checkClosed();
        return new ClientStorageCommand(this);
    }

    @Override
    public SQLCommand prepareSQLCommand(String sql, int fetchSize) {
        checkClosed();
        return new ClientPreparedSQLCommand(this, sql, fetchSize);
    }

    @Override
    public ReplicaSQLCommand prepareReplicaSQLCommand(String sql, int fetchSize) {
        checkClosed();
        return new ClientPreparedSQLCommand(this, sql, fetchSize);
    }

    @Override
    public void close() {
        if (closed)
            return;
        try {
            RuntimeException closeError = null;
            synchronized (this) {
                try {
                    traceOperation("SESSION_CLOSE", 0);
                    // 只有当前Session有效时服务器端才持有对应的session
                    if (isValid()) {
                        newOut().writeRequestHeader(Session.SESSION_CLOSE).flush();
                        tcpConnection.removeSession(sessionId);
                    }
                } catch (RuntimeException e) {
                    trace.error(e, "close");
                    closeError = e;
                } catch (Exception e) {
                    trace.error(e, "close");
                }
                closeTraceSystem();
            }
            tcpConnection = null;
            if (closeError != null) {
                throw closeError;
            }
        } finally {
            super.close();
        }
    }

    public Trace getTrace() {
        return trace;
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
            TransferOutputStream out = newOut();
            int packetId = getNextId();
            traceOperation("LOB_READ", (int) lobId);
            out.writeRequestHeader(packetId, Session.COMMAND_READ_LOB);
            out.writeLong(lobId);
            out.writeBytes(hmac);
            out.writeLong(offset);
            out.writeInt(length);
            return out.flushAndAwait(packetId, new AsyncCallback<Integer>() {
                @Override
                public void runInternal(TransferInputStream in) throws Exception {
                    int length = in.readInt();
                    if (length > 0) {
                        in.readBytes(buff, off, length);
                    }
                    setResult(length);
                }
            });
        } catch (IOException e) {
            handleException(e);
        }
        return 1;
    }

    @Override
    public synchronized void commitTransaction(String allLocalTransactionNames) {
        checkClosed();
        try {
            newOut().writeRequestHeader(Session.COMMAND_DISTRIBUTED_TRANSACTION_COMMIT)
                    .writeString(allLocalTransactionNames).flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void rollbackTransaction() {
        try {
            newOut().writeRequestHeader(Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK).flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void addSavepoint(String name) {
        try {
            newOut().writeRequestHeader(Session.COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT).writeString(name)
                    .flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void rollbackToSavepoint(String name) {
        try {
            newOut().writeRequestHeader(Session.COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT).writeString(name)
                    .flush();
        } catch (IOException e) {
            handleException(e);
        }
    }

    @Override
    public synchronized boolean validateTransaction(String localTransactionName) {
        try {
            TransferOutputStream out = newOut();
            int packetId = getNextId();
            out.writeRequestHeader(packetId, Session.COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE);
            out.writeString(localTransactionName);
            return out.flushAndAwait(packetId, new AsyncCallback<Boolean>() {
                @Override
                public void runInternal(TransferInputStream in) throws Exception {
                    setResult(in.readBoolean());
                }
            });
        } catch (Exception e) {
            handleException(e);
            return false;
        }
    }

    @Override
    public String checkReplicationConflict(String mapName, ByteBuffer key, String replicationName) {
        try {
            TransferOutputStream out = newOut();
            int packetId = getNextId();
            out.writeRequestHeader(packetId, Session.COMMAND_REPLICATION_CHECK_CONFLICT);
            out.writeString(mapName).writeByteBuffer(key).writeString(replicationName);
            return out.flushAndAwait(packetId, new AsyncCallback<String>() {
                @Override
                public void runInternal(TransferInputStream in) throws Exception {
                    setResult(in.readString());
                }
            });
        } catch (Exception e) {
            handleException(e);
            return null;
        }
    }

    @Override
    public void handleReplicationConflict(String mapName, ByteBuffer key, String replicationName) {
        try {
            TransferOutputStream out = newOut();
            int packetId = getNextId();
            out.writeRequestHeader(packetId, Session.COMMAND_REPLICATION_HANDLE_CONFLICT);
            out.writeString(mapName).writeByteBuffer(key).writeString(replicationName).flush();
        } catch (Exception e) {
            handleException(e);
        }
    }

    @Override
    public String getURL() {
        return ci.getURL();
    }

    @Override
    public ParsedSQLStatement parseStatement(String sql) {
        return null;
    }

    @Override
    public PreparedSQLStatement prepareStatement(String sql, int fetchSize) {
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

    @Override
    public void runModeChanged(String newTargetNodes) {
        parent.runModeChanged(newTargetNodes);
    }

    @Override
    public int getNetworkTimeout() {
        return ci.getNetworkTimeout();
    }

    @Override
    public String getLocalHostAndPort() {
        try {
            SocketAddress sa = tcpConnection.getWritableChannel().getSocketChannel().getLocalAddress();
            String host;
            int port;
            if (sa instanceof InetSocketAddress) {
                InetSocketAddress address = (InetSocketAddress) sa;
                host = address.getHostString();
                port = address.getPort();
            } else {
                host = InetAddress.getLocalHost().getHostAddress();
                port = 0;
            }
            return host + ":" + port;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

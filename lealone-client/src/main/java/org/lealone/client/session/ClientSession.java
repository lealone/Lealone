/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.session;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.lealone.client.command.ClientPreparedSQLCommand;
import org.lealone.client.command.ClientSQLCommand;
import org.lealone.client.storage.ClientLobStorage;
import org.lealone.client.storage.ClientStorageCommand;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.exceptions.LealoneException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.TempFileDeleter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.DataHandler;
import org.lealone.db.SysProperties;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.Future;
import org.lealone.db.session.Session;
import org.lealone.db.session.SessionBase;
import org.lealone.net.NetInputStream;
import org.lealone.net.TcpClientConnection;
import org.lealone.net.TransferOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.AckPacketHandler;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketDecoders;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.dt.DTransactionAddSavepoint;
import org.lealone.server.protocol.dt.DTransactionCommit;
import org.lealone.server.protocol.dt.DTransactionRollback;
import org.lealone.server.protocol.dt.DTransactionRollbackSavepoint;
import org.lealone.server.protocol.lob.LobRead;
import org.lealone.server.protocol.lob.LobReadAck;
import org.lealone.server.protocol.session.SessionCancelStatement;
import org.lealone.server.protocol.session.SessionClose;
import org.lealone.server.protocol.session.SessionSetAutoCommit;
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
// 一个ClientSession对应一条JdbcConnection，多个ClientSession其用一个TcpClientConnection。
// 同JdbcConnection一样，每个ClientSession对象也不是线程安全的，只能在单线程中使用。
// 另外，每个ClientSession只对应一个server，
// 虽然ConnectionInfo允许在JDBC URL中指定多个server，但是放在ClientSessionFactory中处理了。
public class ClientSession extends SessionBase implements DataHandler, Transaction.Participant {

    private final TcpClientConnection tcpConnection;
    private final ConnectionInfo ci;
    private final String server;
    private final Session parent;
    private final int id;
    private final String cipher;
    private final byte[] fileEncryptionKey;
    private final Trace trace;
    private final Object lobSyncObject = new Object();
    private LobStorage lobStorage;

    ClientSession(TcpClientConnection tcpConnection, ConnectionInfo ci, String server, Session parent, int id) {
        this.tcpConnection = tcpConnection;
        this.ci = ci;
        this.server = server;
        this.parent = parent;
        this.id = id;

        cipher = ci.getProperty("CIPHER");
        fileEncryptionKey = cipher == null ? null : MathUtils.secureRandomBytes(32);

        initTraceSystem(ci);
        trace = traceSystem == null ? Trace.NO_TRACE : traceSystem.getTrace(TraceModuleType.JDBC);
    }

    @Override
    public String toString() {
        return "ClientSession[" + id + ", " + server + "]";
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public int getNextId() {
        checkClosed();
        return tcpConnection.getNextId();
    }

    @Override
    public int getCurrentId() {
        return tcpConnection.getCurrentId();
    }

    InetSocketAddress getInetSocketAddress() {
        return tcpConnection.getInetSocketAddress();
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
            send(new SessionCancelStatement(statementId));
        } catch (Exception e) {
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
            traceOperation("SESSION_SET_AUTOCOMMIT", autoCommit ? 1 : 0);
            send(new SessionSetAutoCommit(autoCommit));
        } catch (Exception e) {
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
                    // 只有当前Session有效时服务器端才持有对应的session
                    if (isValid()) {
                        send(new SessionClose());
                        tcpConnection.removeSession(id);
                    }
                } catch (RuntimeException e) {
                    trace.error(e, "close");
                    closeError = e;
                } catch (Exception e) {
                    trace.error(e, "close");
                }
                closeTraceSystem();
            }
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
    public synchronized int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off, int length) {
        try {
            LobReadAck ack = this.<LobReadAck> send(new LobRead(lobId, hmac, offset, length)).get();
            if (ack.buff != null && ack.buff.length < 0) {
                System.arraycopy(ack.buff, 0, buff, off, length);
            }
        } catch (Exception e) {
            handleException(e);
        }
        return 1;
    }

    @Override
    public synchronized void commitTransaction(String allLocalTransactionNames) {
        checkClosed();
        try {
            send(new DTransactionCommit(allLocalTransactionNames));
        } catch (Exception e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void rollbackTransaction() {
        try {
            send(new DTransactionRollback());
        } catch (Exception e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void addSavepoint(String name) {
        try {
            send(new DTransactionAddSavepoint(name));
        } catch (Exception e) {
            handleException(e);
        }
    }

    @Override
    public synchronized void rollbackToSavepoint(String name) {
        try {
            send(new DTransactionRollbackSavepoint(name));
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

    @Override
    public <R, P extends AckPacket> Future<R> send(Packet packet, AckPacketHandler<R, P> ackPacketHandler) {
        int packetId = getNextId();
        return send(packet, packetId, ackPacketHandler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R, P extends AckPacket> Future<R> send(Packet packet, int packetId,
            AckPacketHandler<R, P> ackPacketHandler) {
        traceOperation(packet.getType().name(), packetId);
        AsyncCallback<R> ac = new AsyncCallback<R>() {
            @Override
            public void runInternal(NetInputStream in) throws Exception {
                PacketDecoder<? extends Packet> decoder = PacketDecoders.getDecoder(packet.getAckType());
                Packet packet = decoder.decode(in, getProtocolVersion());
                if (ackPacketHandler != null) {
                    try {
                        setAsyncResult(ackPacketHandler.handle((P) packet));
                    } catch (Throwable e) {
                        setAsyncResult(e);
                    }
                }
            }
        };
        if (packet.getAckType() != PacketType.VOID) {
            tcpConnection.addAsyncCallback(packetId, ac);
        }
        try {
            TransferOutputStream out = newOut();
            out.writeRequestHeader(packetId, packet.getType());
            packet.encode(out, getProtocolVersion());
            out.flush();
        } catch (Throwable e) {
            ac.setAsyncResult(e);
        }
        return ac;
    }
}

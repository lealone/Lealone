/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.session;

import java.net.InetSocketAddress;

import org.lealone.client.command.ClientPreparedSQLCommand;
import org.lealone.client.command.ClientSQLCommand;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.DataHandler;
import org.lealone.db.DbSetting;
import org.lealone.db.LocalDataHandler;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.Future;
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
import org.lealone.server.protocol.lob.LobRead;
import org.lealone.server.protocol.lob.LobReadAck;
import org.lealone.server.protocol.session.SessionCancelStatement;
import org.lealone.server.protocol.session.SessionClose;
import org.lealone.server.protocol.session.SessionSetAutoCommit;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.lob.LobLocalStorage;

/**
 * The client side part of a session when using the server mode. 
 * This object communicates with a session on the server side.
 * 
 * @author H2 Group
 * @author zhh
 */
// 一个ClientSession对应一条JdbcConnection，多个ClientSession共用一个TcpClientConnection。
// 同JdbcConnection一样，每个ClientSession对象也不是线程安全的，只能在单线程中使用。
// 另外，每个ClientSession只对应一个server，
// 虽然ConnectionInfo允许在JDBC URL中指定多个server，但是放在ClientSessionFactory中处理了。
public class ClientSession extends SessionBase implements LobLocalStorage.LobReader {

    private final TcpClientConnection tcpConnection;
    private final ConnectionInfo ci;
    private final String server;
    private final int id;
    private final LocalDataHandler dataHandler;
    private final Trace trace;

    ClientSession(TcpClientConnection tcpConnection, ConnectionInfo ci, String server, int id) {
        this.tcpConnection = tcpConnection;
        this.ci = ci;
        this.server = server;
        this.id = id;

        String cipher = ci.getProperty(DbSetting.CIPHER.getName());
        dataHandler = new LocalDataHandler(cipher);
        dataHandler.setLobReader(this);

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

    public int getNextId() {
        checkClosed();
        return tcpConnection.getNextId();
    }

    public int getCurrentId() {
        return tcpConnection.getCurrentId();
    }

    InetSocketAddress getInetSocketAddress() {
        return tcpConnection.getInetSocketAddress();
    }

    @Override
    public void checkClosed() {
        if (tcpConnection.isClosed()) {
            String msg = tcpConnection.getInetSocketAddress().getHostName() + " tcp connection closed";
            throw getConnectionBrokenException(msg);
        }
        if (isClosed()) {
            throw getConnectionBrokenException("session closed");
        }
    }

    private DbException getConnectionBrokenException(String msg) {
        return DbException.get(ErrorCode.CONNECTION_BROKEN_1, tcpConnection.getPendingException(), msg);
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

    public void handleException(Throwable e) {
        checkClosed();
        if (e instanceof DbException)
            throw (DbException) e;
        throw DbException.convert(e);
    }

    @Override
    public SQLCommand createSQLCommand(String sql, int fetchSize) {
        checkClosed();
        return new ClientSQLCommand(this, sql, fetchSize);
    }

    @Override
    public SQLCommand prepareSQLCommand(String sql, int fetchSize) {
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
                    send(new SessionClose());
                    tcpConnection.removeSession(id);
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
    public DataHandler getDataHandler() {
        return dataHandler;
    }

    @Override
    public synchronized int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off,
            int length) {
        try {
            LobReadAck ack = this.<LobReadAck> send(new LobRead(lobId, hmac, offset, length)).get();
            if (ack.buff != null && ack.buff.length > 0) {
                System.arraycopy(ack.buff, 0, buff, off, ack.buff.length);
                return ack.buff.length;
            }
        } catch (Exception e) {
            handleException(e);
        }
        return -1;
    }

    @Override
    public ConnectionInfo getConnectionInfo() {
        return ci;
    }

    @Override
    public void setNetworkTimeout(int milliseconds) {
        ci.setNetworkTimeout(milliseconds);
    }

    @Override
    public int getNetworkTimeout() {
        return ci.getNetworkTimeout();
    }

    @SuppressWarnings("unchecked")
    public <P extends AckPacket> Future<P> send(Packet packet) {
        return send(packet, p -> {
            return (P) p;
        });
    }

    @SuppressWarnings("unchecked")
    public <P extends AckPacket> Future<P> send(Packet packet, int packetId) {
        return send(packet, packetId, p -> {
            return (P) p;
        });
    }

    public <R, P extends AckPacket> Future<R> send(Packet packet,
            AckPacketHandler<R, P> ackPacketHandler) {
        int packetId = getNextId();
        return send(packet, packetId, ackPacketHandler);
    }

    @SuppressWarnings("unchecked")
    public <R, P extends AckPacket> Future<R> send(Packet packet, int packetId,
            AckPacketHandler<R, P> ackPacketHandler) {
        traceOperation(packet.getType().name(), packetId);
        AsyncCallback<R> ac;
        if (packet.getAckType() != PacketType.VOID) {
            ac = new AsyncCallback<R>() {
                @Override
                public void runInternal(NetInputStream in) throws Exception {
                    PacketDecoder<? extends Packet> decoder = PacketDecoders
                            .getDecoder(packet.getAckType());
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
            ac.setPacket(packet);
            ac.setStartTime(System.currentTimeMillis());
            ac.setNetworkTimeout(getNetworkTimeout());
            tcpConnection.addAsyncCallback(packetId, ac);
        } else {
            ac = null;
        }
        try {
            checkClosed();
            TransferOutputStream out = tcpConnection.createTransferOutputStream(this);
            out.writeRequestHeader(packetId, packet.getType());
            packet.encode(out, getProtocolVersion());
            out.flush();
        } catch (Throwable e) {
            if (ac != null) {
                removeAsyncCallback(packetId);
                ac.setAsyncResult(e);
            } else {
                handleException(e);
            }
        }
        return ac;
    }

    private void removeAsyncCallback(int packetId) {
        tcpConnection.removeAsyncCallback(packetId);
    }
}

/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.client.session;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.lealone.client.command.ClientPreparedSQLCommand;
import com.lealone.client.command.ClientSQLCommand;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.trace.Trace;
import com.lealone.common.trace.TraceModuleType;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.DataHandler;
import com.lealone.db.DbSetting;
import com.lealone.db.LocalDataHandler;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;
import com.lealone.db.async.SingleThreadAsyncCallback;
import com.lealone.db.command.SQLCommand;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.db.session.SessionBase;
import com.lealone.net.NetInputStream;
import com.lealone.net.TcpClientConnection;
import com.lealone.net.TransferOutputStream;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.AckPacketHandler;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketDecoders;
import com.lealone.server.protocol.PacketType;
import com.lealone.server.protocol.lob.LobRead;
import com.lealone.server.protocol.lob.LobReadAck;
import com.lealone.server.protocol.session.SessionCancelStatement;
import com.lealone.server.protocol.session.SessionClose;
import com.lealone.server.protocol.session.SessionSetAutoCommit;
import com.lealone.storage.lob.LobLocalStorage;

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

    private final boolean isBio;
    private final TransferOutputStream out; // 如果是阻塞io，输出流的buffer可以复用

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

        isBio = tcpConnection.getWritableChannel().isBio();
        out = tcpConnection.getTransferOutputStream();
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
    public boolean isClosed() {
        return closed || tcpConnection.isClosed();
    }

    @Override
    public void checkClosed() {
        if (tcpConnection.isClosed()) {
            String msg = tcpConnection.getWritableChannel().getHost() + " tcp connection closed";
            throw getConnectionBrokenException(msg);
        }
        if (closed) {
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
    public SQLCommand createSQLCommand(String sql, int fetchSize, boolean prepared) {
        checkClosed();
        if (prepared)
            return new ClientPreparedSQLCommand(this, sql, fetchSize);
        else
            return new ClientSQLCommand(this, sql, fetchSize);
    }

    @Override
    public void close() {
        if (isClosed())
            return;
        AsyncCallback<Void> ac = createCallback();
        execute(ac, () -> {
            Throwable closeError = null;
            try {
                // 只有当前Session有效时服务器端才持有对应的session
                if (isValid()) {
                    send(new SessionClose());
                    tcpConnection.removeSession(id);
                }
                if (getScheduler() != null) {
                    getScheduler().removeSession(this);
                }
                super.close();
                if (isBio()) {
                    tcpConnection.close();
                }
            } catch (RuntimeException e) {
                trace.error(e, "close");
                closeError = e;
            } catch (Exception e) {
                trace.error(e, "close");
            }
            try {
                closeTraceSystem();
            } catch (Exception e) {
                closeError = e;
            }
            if (closeError != null) {
                ac.setAsyncResult(closeError);
            } else {
                ac.setAsyncResult((Void) null);
            }
        });
        ac.get();
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
            AsyncCallback<Integer> ac = createCallback();
            execute(ac, () -> {
                LobReadAck ack = this.<LobReadAck> send(new LobRead(lobId, hmac, offset, length)).get();
                if (ack.buff != null && ack.buff.length > 0) {
                    System.arraycopy(ack.buff, 0, buff, off, ack.buff.length);
                    ac.setAsyncResult(ack.buff.length);
                }
            });
            return ac.get();
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

    @Override
    public <R, P extends AckPacket> Future<R> send(Packet packet,
            AckPacketHandler<R, P> ackPacketHandler) {
        int packetId = getNextId();
        return send(packet, packetId, ackPacketHandler);
    }

    @Override
    public <R, P extends AckPacket> Future<R> send(Packet packet, int packetId,
            AckPacketHandler<R, P> ackPacketHandler) {
        if (DbException.ASSERT) {
            DbException.assertTrue(
                    getScheduler() == null || getScheduler() == SchedulerThread.currentScheduler());
        }
        traceOperation(packet.getType().name(), packetId);
        AsyncCallback<R> ac;
        if (packet.getAckType() != PacketType.VOID) {
            ac = new SingleThreadAsyncCallback<R>() {
                @Override
                public void runInternal(NetInputStream in) throws Exception {
                    handleAsyncCallback(in, packet.getAckType(), ackPacketHandler, this);
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
            out.writeRequestHeader(this, packetId, packet.getType());
            packet.encode(out, getProtocolVersion());
            out.flush();
            if (ac != null && isBio)
                tcpConnection.getWritableChannel().read();
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

    @SuppressWarnings("unchecked")
    private <R, P extends AckPacket> void handleAsyncCallback(NetInputStream in, PacketType packetType,
            AckPacketHandler<R, P> ackPacketHandler, AsyncCallback<R> ac) throws IOException {
        try {
            PacketDecoder<? extends Packet> decoder = PacketDecoders.getDecoder(packetType);
            Packet packet = decoder.decode(in, getProtocolVersion());
            if (ackPacketHandler != null) {
                R r = ackPacketHandler.handle((P) packet);
                ac.setAsyncResult(r);
            }
        } catch (Throwable e) {
            ac.setAsyncResult(e);
        }
    }

    // 外部插件会用到，所以独立出一个public方法
    public void removeAsyncCallback(int packetId) {
        tcpConnection.removeAsyncCallback(packetId);
    }

    @Override
    public <T> AsyncCallback<T> createCallback() {
        return AsyncCallback.create(isBio() || SchedulerThread.isScheduler());
    }

    @Override
    public boolean isBio() {
        return isBio;
    }

    private Scheduler scheduler;

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }
}

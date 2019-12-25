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
package org.lealone.net;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.RunMode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.Session;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.session.SessionInit;
import org.lealone.server.protocol.session.SessionInitAck;

/**
 * An async tcp client connection.
 */
public class TcpClientConnection extends TransferConnection {

    private static final Logger logger = LoggerFactory.getLogger(TcpClientConnection.class);

    private final ConcurrentHashMap<Integer, Session> sessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, AsyncCallback<?>> callbackMap = new ConcurrentHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger(0);
    // private final NetClient netClient;

    public TcpClientConnection(WritableChannel writableChannel, NetClient netClient) {
        super(writableChannel, false);
        // this.netClient = netClient;
    }

    public int getNextId() {
        return nextId.incrementAndGet();
    }

    public int getCurrentId() {
        return nextId.get();
    }

    @Override
    public void addAsyncCallback(int packetId, AsyncCallback<?> ac) {
        callbackMap.put(packetId, ac);
    }

    @Override
    public void close() {
        // 如果还有回调未处理需要设置异常，避免等待回调结果的线程一直死等
        if (!callbackMap.isEmpty()) {
            DbException e = DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                    "unexpected status " + Session.STATUS_CLOSED);
            for (AsyncCallback<?> callback : callbackMap.values()) {
                callback.setDbException(e, true);
            }
        }
        super.close();
    }

    private Session getSession(int sessionId) {
        return sessions.get(sessionId);
    }

    public void addSession(int sessionId, Session session) {
        sessions.put(sessionId, session);
    }

    public Session removeSession(int sessionId) {
        Session session = sessions.remove(sessionId);
        // 不在这里删除连接，这会导致很多问题
        // if (netClient != null && sessions.isEmpty()) {
        // netClient.removeConnection(inetSocketAddress);
        // }
        return session;
    }

    public void writeInitPacket(final Session session) throws Exception {
        checkClosed();
        ConnectionInfo ci = session.getConnectionInfo();
        int packetId = getNextId();
        TransferOutputStream out = createTransferOutputStream(session);
        out.setSSL(ci.isSSL());
        out.writeRequestHeader(packetId, PacketType.SESSION_INIT.value);
        out.writeInt(Constants.TCP_PROTOCOL_VERSION_1); // minClientVersion
        out.writeInt(Constants.TCP_PROTOCOL_VERSION_1); // maxClientVersion
        out.writeString(ci.getDatabaseShortName());
        out.writeString(ci.getURL()); // 不带参数的URL
        out.writeString(ci.getUserName());
        out.writeBytes(ci.getUserPasswordHash());
        out.writeBytes(ci.getFilePasswordHash());
        out.writeBytes(ci.getFileEncryptionKey());
        String[] keys = ci.getKeys();
        out.writeInt(keys.length);
        for (String key : keys) {
            out.writeString(key).writeString(ci.getProperty(key));
        }
        out.flushAndAwait(packetId, ci.getNetworkTimeout(), new AsyncCallback<Void>() {
            @Override
            public void runInternal(NetInputStream in) throws Exception {
                int protocolVersion = in.readInt();
                boolean autoCommit = in.readBoolean();
                session.setProtocolVersion(protocolVersion);
                session.setAutoCommit(autoCommit);
                session.setTargetNodes(in.readString());
                session.setRunMode(RunMode.valueOf(in.readString()));
                session.setInvalid(in.readBoolean());
            }
        });
    }

    public void writeInitPacketAsync(final Session session, AsyncHandler<AsyncResult<Session>> asyncHandler) {
        checkClosed();
        ConnectionInfo ci = session.getConnectionInfo();
        SessionInit packet = new SessionInit(ci);
        session.<SessionInitAck> sendAsync(packet, ack -> {
            session.setProtocolVersion(ack.clientVersion);
            session.setAutoCommit(ack.autoCommit);
            session.setTargetNodes(ack.targetNodes);
            session.setRunMode(ack.runMode);
            session.setInvalid(ack.invalid);
            asyncHandler.handle(new AsyncResult<>(session));
        });
    }

    @Override
    protected void handleResponse(TransferInputStream in, int packetId, int status) throws IOException {
        checkClosed();
        String newTargetNodes = null;
        Session session = null;
        DbException e = null;
        if (status == Session.STATUS_OK) {
            // ok
        } else if (status == Session.STATUS_ERROR) {
            e = parseError(in);
        } else if (status == Session.STATUS_CLOSED) {
            in = null;
        } else if (status == Session.STATUS_RUN_MODE_CHANGED) {
            int sessionId = in.readInt();
            session = getSession(sessionId);
            newTargetNodes = in.readString();
        } else {
            e = DbException.get(ErrorCode.CONNECTION_BROKEN_1, "unexpected status " + status);
        }

        AsyncCallback<?> ac = callbackMap.remove(packetId);
        if (ac == null) {
            String msg = "Async callback is null, may be a bug! packetId = " + packetId;
            if (e != null) {
                logger.warn(msg, e);
            } else {
                logger.warn(msg);
            }
            return;
        }
        if (e != null)
            ac.setDbException(e);
        ac.run(in);
        if (newTargetNodes != null)
            session.runModeChanged(newTargetNodes);
    }
}

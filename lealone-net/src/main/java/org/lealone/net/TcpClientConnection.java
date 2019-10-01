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
import org.lealone.db.Session;
import org.lealone.db.api.ErrorCode;

/**
 * An async tcp client connection.
 */
public class TcpClientConnection extends TransferConnection {

    private static final Logger logger = LoggerFactory.getLogger(TcpClientConnection.class);

    private final ConcurrentHashMap<Integer, Session> sessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, AsyncCallback<?>> callbackMap = new ConcurrentHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger(0);
    private final NetClient netClient;

    public TcpClientConnection(WritableChannel writableChannel, NetClient netClient) {
        super(writableChannel, false);
        this.netClient = netClient;
    }

    public int getNextId() {
        return nextId.incrementAndGet();
    }

    @Override
    protected void addAsyncCallback(int id, AsyncCallback<?> ac) {
        callbackMap.put(id, ac);
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
        if (netClient != null && sessions.isEmpty()) {
            netClient.removeConnection(inetSocketAddress);
        }
        return session;
    }

    public Transfer createTransfer(Session session) {
        return new Transfer(this, writableChannel, session);
    }

    public void writeInitPacket(Session session, Transfer transfer, ConnectionInfo ci) throws Exception {
        checkClosed();
        int id = session.getNextId();
        transfer.setSSL(ci.isSSL());
        transfer.writeRequestHeader(id, Session.SESSION_INIT);
        transfer.writeInt(Constants.TCP_PROTOCOL_VERSION_1); // minClientVersion
        transfer.writeInt(Constants.TCP_PROTOCOL_VERSION_1); // maxClientVersion
        transfer.writeString(ci.getDatabaseShortName());
        transfer.writeString(ci.getURL()); // 不带参数的URL
        transfer.writeString(ci.getUserName());
        transfer.writeBytes(ci.getUserPasswordHash());
        transfer.writeBytes(ci.getFilePasswordHash());
        transfer.writeBytes(ci.getFileEncryptionKey());
        String[] keys = ci.getKeys();
        transfer.writeInt(keys.length);
        for (String key : keys) {
            transfer.writeString(key).writeString(ci.getProperty(key));
        }
        AsyncCallback<Void> ac = new AsyncCallback<Void>() {
            @Override
            public void runInternal() {
                try {
                    int clientVersion = transfer.readInt();
                    transfer.setVersion(clientVersion);
                    boolean autoCommit = transfer.readBoolean();
                    session.setAutoCommit(autoCommit);
                    session.setTargetEndpoints(transfer.readString());
                    session.setRunMode(RunMode.valueOf(transfer.readString()));
                    session.setInvalid(transfer.readBoolean());
                } catch (IOException e) {
                    throw DbException.convert(e);
                }
            }
        };
        transfer.addAsyncCallback(id, ac);
        transfer.flush();
        ac.await(ci.getNetworkTimeout());
    }

    @Override
    protected void handleResponse(Transfer transfer, int id, int status) throws IOException {
        checkClosed();
        String newTargetEndpoints = null;
        Session session = null;
        DbException e = null;
        if (status == Session.STATUS_OK) {
            // ok
        } else if (status == Session.STATUS_ERROR) {
            e = parseError(transfer);
        } else if (status == Session.STATUS_CLOSED) {
            transfer = null;
        } else if (status == Session.STATUS_RUN_MODE_CHANGED) {
            int sessionId = transfer.readInt();
            session = getSession(sessionId);
            newTargetEndpoints = transfer.readString();
        } else {
            e = DbException.get(ErrorCode.CONNECTION_BROKEN_1, "unexpected status " + status);
        }

        AsyncCallback<?> ac = callbackMap.remove(id);
        if (ac == null) {
            String msg = "Async callback is null, may be a bug! id = " + id;
            if (e != null) {
                logger.warn(msg, e);
            } else {
                logger.warn(msg);
            }
            return;
        }
        if (e != null)
            ac.setDbException(e);
        ac.run(transfer);
        if (newTargetEndpoints != null)
            session.runModeChanged(newTargetEndpoints);
    }
}

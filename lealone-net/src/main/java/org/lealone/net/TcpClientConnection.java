/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.session.Session;

/**
 * An async tcp client connection.
 */
public class TcpClientConnection extends TransferConnection {

    private static final Logger logger = LoggerFactory.getLogger(TcpClientConnection.class);

    private final Map<Integer, Session> sessions;
    private final Map<Integer, AsyncCallback<?>> callbackMap;
    private final AtomicInteger nextId = new AtomicInteger(0);
    private final int maxSharedSize;
    private final NetClient netClient;

    private Throwable pendingException;

    public TcpClientConnection(WritableChannel writableChannel, NetClient netClient, int maxSharedSize) {
        super(writableChannel, false);
        this.netClient = netClient;
        this.maxSharedSize = maxSharedSize;
        if (netClient.isThreadSafe()) {
            sessions = new HashMap<>();
            callbackMap = new HashMap<>();
        } else {
            sessions = new ConcurrentHashMap<>();
            callbackMap = new ConcurrentHashMap<>();
        }
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

    public void removeAsyncCallback(int packetId) {
        callbackMap.remove(packetId);
    }

    @Override
    public void close() {
        // 如果还有回调未处理需要设置异常，避免等待回调结果的线程一直死等
        if (!callbackMap.isEmpty()) {
            DbException e;
            if (pendingException != null) {
                e = DbException.convert(pendingException);
                pendingException = null;
            } else {
                e = DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                        "unexpected status " + Session.STATUS_CLOSED);
            }
            for (AsyncCallback<?> callback : callbackMap.values()) {
                callback.setDbException(e, true);
            }
        }
        super.close();

        for (Session s : sessions.values()) {
            try {
                s.close();
            } catch (Exception e) { // 忽略异常
            }
        }
        sessions.clear();
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
            ac.setAsyncResult(e);
        else
            ac.run(in);
        if (newTargetNodes != null)
            session.runModeChanged(newTargetNodes);
    }

    @Override
    public void handleException(Exception e) {
        pendingException = e;
        netClient.removeConnection(this);
    }

    public Throwable getPendingException() {
        return pendingException;
    }

    @Override
    public void checkTimeout(long currentTime) {
        for (AsyncCallback<?> ac : callbackMap.values()) {
            ac.checkTimeout(currentTime);
        }
    }

    @Override
    public boolean isShared() {
        return true;
    }

    @Override
    public int getSharedSize() {
        return sessions.size();
    }

    @Override
    public int getMaxSharedSize() {
        return maxSharedSize;
    }
}

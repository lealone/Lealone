/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import org.lealone.db.link.LinkableBase;
import org.lealone.server.protocol.session.SessionInit;

//如果数据库还没有初始化，只会有一个线程去初始化它，这时其他线程就不能创建session，会返回null，所以可能会run多次。
public class SessionInitTask extends LinkableBase<SessionInitTask> {

    private final TcpServerConnection conn;
    private final SessionInit packet;
    private final int packetId;
    private final int sessionId;

    public SessionInitTask(TcpServerConnection conn, SessionInit packet, int packetId, int sessionId) {
        this.conn = conn;
        this.packet = packet;
        this.packetId = packetId;
        this.sessionId = sessionId;
    }

    public boolean run() {
        return conn.createSession(packet, packetId, sessionId);
    }

    public SessionInitTask copy() {
        return new SessionInitTask(conn, packet, packetId, sessionId);
    }
}

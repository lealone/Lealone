/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.scheduler;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.db.session.ServerSession;
import com.lealone.server.TcpServerConnection;
import com.lealone.server.handler.PacketHandler;
import com.lealone.server.protocol.Packet;

public class PacketHandleTask extends LinkableTask {

    private static final Logger logger = LoggerFactory.getLogger(PacketHandleTask.class);

    public final TcpServerConnection conn;
    public final int packetId;
    public final ServerSession session;
    public final int sessionId;
    public final ServerSessionInfo si;
    private final Packet packet;
    private final PacketHandler<Packet> handler;

    public PacketHandleTask(TcpServerConnection conn, int packetId, ServerSessionInfo si, Packet packet,
            PacketHandler<Packet> handler) {
        this.conn = conn;
        this.packetId = packetId;
        this.session = si.getSession();
        this.sessionId = si.getSessionId();
        this.si = si;
        this.packet = packet;
        this.handler = handler;
    }

    @Override
    public void run() {
        try {
            Packet ack = handler.handle(this, packet);
            if (ack != null) {
                conn.sendResponse(this, ack);
            }
        } catch (Throwable e) {
            String message = "Failed to handle packet, packetId: {}, packetType: {}, sessionId: {}";
            logger.error(message, e, packetId, packet.getType(), sessionId);
            conn.sendError(session, packetId, e);
        }
    }
}

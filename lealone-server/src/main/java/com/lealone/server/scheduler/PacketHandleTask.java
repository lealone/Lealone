/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.scheduler;

import java.sql.SQLException;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.db.session.ServerSession;
import com.lealone.server.handler.PacketHandler;
import com.lealone.server.protocol.Packet;
import com.lealone.sql.PreparedSQLStatement;

public class PacketHandleTask extends LinkableTask {

    private static final Logger logger = LoggerFactory.getLogger(PacketHandleTask.class);

    public final int packetId;
    public final ServerSession session;
    private final ServerSessionInfo si;
    private final Packet packet;
    private final PacketHandler<Packet> handler;

    public PacketHandleTask(int packetId, ServerSessionInfo si, Packet packet,
            PacketHandler<Packet> handler) {
        this.packetId = packetId;
        this.session = si.getSession();
        this.si = si;
        this.packet = packet;
        this.handler = handler;
    }

    @Override
    public void run() {
        try {
            Packet ack = handler.handle(this, packet);
            if (ack != null) {
                sendResponse(ack);
            }
        } catch (Throwable e) {
            String message = "Failed to handle packet, packetId: {}, packetType: {}, sessionId: {}";
            if (e.getCause() instanceof SQLException) {
                if (logger.isDebugEnabled())
                    logger.debug(message, e, packetId, packet.getType(), si.getSessionId());
            } else {
                logger.error(message, e, packetId, packet.getType(), si.getSessionId());
            }
            sendError(e);
        }
    }

    public ServerSessionInfo si() {
        return si;
    }

    public void closeSession() {
        si.getConnection().closeSession(packetId, si.getSessionId());
    }

    public void sendResponse(Packet ack) {
        si.getConnection().sendResponse(this, ack);
    }

    public void sendError(Throwable t) {
        si.getConnection().sendError(session, packetId, t);
    }

    public void submitYieldableCommand(PreparedSQLStatement.Yieldable<?> yieldable) {
        si.submitYieldableCommand(packetId, yieldable);
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.session.ServerSession;
import org.lealone.net.TransferInputStream;
import org.lealone.server.handler.PacketHandler;
import org.lealone.server.handler.PacketHandlers;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketDecoders;

public class PacketDeliveryTask implements AsyncTask {

    private static final Logger logger = LoggerFactory.getLogger(PacketDeliveryTask.class);

    public final TcpServerConnection conn;
    public final TransferInputStream in;
    public final int packetId;
    public final int packetType;
    public final ServerSession session;
    public final int sessionId;
    public final SessionInfo si;

    public PacketDeliveryTask(TcpServerConnection conn, TransferInputStream in, int packetId, int packetType,
            SessionInfo si) {
        this.conn = conn;
        this.in = in;
        this.packetId = packetId;
        this.packetType = packetType;
        this.session = si.getSession();
        this.sessionId = si.getSessionId();
        this.si = si;
    }

    @Override
    public void run() {
        try {
            handlePacket();
        } catch (Throwable e) {
            String message = "Failed to handle packet, packetId: {}, packetType: {}, sessionId: {}";
            logger.error(message, e, packetId, packetType, sessionId);
            conn.sendError(session, packetId, e);
        } finally {
            // 确保无论出现什么情况都能关闭，调用closeInputStream两次也是无害的
            // in.closeInputStream();
        }
    }

    // TODO 调用in.closeInputStream()还有bug，会导致奇怪的问题，运行TableLockTest那个例子偶尔能重现问题
    private void handlePacket() throws Exception {
        int version = session.getProtocolVersion();
        PacketDecoder<? extends Packet> decoder = PacketDecoders.getDecoder(packetType);
        Packet packet = decoder.decode(in, version);
        // in.closeInputStream(); // 到这里输入流已经读完，及时释放NetBuffer
        @SuppressWarnings("unchecked")
        PacketHandler<Packet> handler = PacketHandlers.getHandler(packetType);
        if (handler != null) {
            Packet ack = handler.handle(this, packet);
            if (ack != null) {
                conn.sendResponse(this, ack);
            }
        } else {
            logger.warn("Unknow packet type: {}", packetType);
        }
    }
}

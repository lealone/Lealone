/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.async.SingleThreadAsyncCallback;
import com.lealone.db.session.Session;
import com.lealone.net.NetInputStream;

public class AckAsyncCallback<R, P extends AckPacket> extends SingleThreadAsyncCallback<R> {

    private final Packet packet;
    private final AckPacketHandler<R, P> ackPacketHandler;
    private final int networkTimeout;
    private final int protocolVersion;

    private long startTime;

    public AckAsyncCallback(Packet packet, AckPacketHandler<R, P> ackPacketHandler, Session session) {
        this.packet = packet;
        this.ackPacketHandler = ackPacketHandler;
        this.networkTimeout = session.getNetworkTimeout();
        this.protocolVersion = session.getProtocolVersion();
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public R get() {
        long timeoutMillis = networkTimeout > 0 ? networkTimeout : -1;
        return await(timeoutMillis);
    }

    public void checkTimeout(long currentTime) {
        if (networkTimeout <= 0 || startTime <= 0 || startTime + networkTimeout > currentTime)
            return;
        String msg = "ack timeout, request start time: " + new java.sql.Timestamp(startTime) //
                + ", network timeout: " + networkTimeout + "ms" //
                + ", request packet: " + packet;
        DbException e = DbException.get(ErrorCode.NETWORK_TIMEOUT_1, msg);
        setAsyncResult(e);
        startTime = 0;
    }

    @SuppressWarnings("unchecked")
    public void handleAckPacket(NetInputStream in) {
        try {
            PacketDecoder<? extends Packet> decoder = PacketDecoders.getDecoder(packet.getAckType());
            Packet ack = decoder.decode(in, protocolVersion);
            R r;
            if (ackPacketHandler != null) {
                r = ackPacketHandler.handle((P) ack);
            } else {
                r = (R) ack;
            }
            setAsyncResult(r);
        } catch (Throwable e) {
            setAsyncResult(e);
        }
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mysql.server.protocol;

public abstract class ResponsePacket extends Packet {

    /**
     * 计算数据包大小，不包含包头长度。
     */
    public abstract int calcPacketSize();

    public abstract void writeBody(PacketOutput out);

    @Override
    public void write(PacketOutput out) {
        writeHeader(out);
        writeBody(out);
        out.flush();
    }

    // write header
    private void writeHeader(PacketOutput out) {
        int size = calcPacketSize();
        out.allocate(4 + size); // PacketHeader占4个字节
        out.writeUB3(size);
        out.write(packetId);
    }
}

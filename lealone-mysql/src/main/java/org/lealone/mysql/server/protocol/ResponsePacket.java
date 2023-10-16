/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mysql.server.protocol;

import java.nio.ByteBuffer;

import org.lealone.mysql.server.util.BufferUtil;

public abstract class ResponsePacket extends Packet {

    /**
     * 计算数据包大小，不包含包头长度。
     */
    public abstract int calcPacketSize();

    public abstract void writeBody(ByteBuffer buffer, PacketOutput out);

    @Override
    public void write(PacketOutput out) {
        int size = calcPacketSize();
        ByteBuffer buffer = out.allocate(4 + size); // PacketHeader占4个字节

        // write header
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);

        writeBody(buffer, out);
        out.flush();
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.bio;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.MapUtils;
import org.lealone.db.DataBuffer;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetBuffer;
import org.lealone.net.NetBufferFactory;
import org.lealone.net.WritableChannel;
import org.lealone.net.nio.NioBuffer;
import org.lealone.net.nio.NioBufferFactory;

public class BioWritableChannel implements WritableChannel {

    private final String host;
    private final int port;
    private final int maxPacketSize;

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    public BioWritableChannel(Map<String, String> config, Socket socket, InetSocketAddress address)
            throws IOException {
        host = address.getHostString();
        port = address.getPort();
        maxPacketSize = MapUtils.getInt(config, "max_packet_size", 8 * 1024 * 1024);

        this.socket = socket;
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024));

    }

    @Override
    public void write(NetBuffer data) {
        if (data instanceof NioBuffer) {
            NioBuffer nioBuffer = (NioBuffer) data;
            ByteBuffer bb = nioBuffer.getByteBuffer();
            try {
                if (bb.hasArray()) {
                    out.write(bb.array(), bb.arrayOffset(), bb.limit());
                } else {
                    byte[] bytes = new byte[bb.limit()];
                    bb.get(bytes);
                    out.write(bytes);
                }
                out.flush();
            } catch (IOException e) {
                throw DbException.convert(e);
            }
        }
    }

    @Override
    public void close() {
        if (socket != null) {
            try {
                socket.close();
            } catch (Throwable t) {
            }
            socket = null;
            in = null;
            out = null;
        }
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public NetBufferFactory getBufferFactory() {
        return NioBufferFactory.getInstance();
    }

    @Override
    public boolean isBio() {
        return true;
    }

    @Override
    public void read(AsyncConnection conn) {
        try {
            if (conn.isClosed())
                return;
            int packetLength = in.readInt();
            if (packetLength > maxPacketSize)
                throw new IOException("packet too large, maxPacketSize: " + maxPacketSize + ", receive: "
                        + packetLength);
            DataBuffer dataBuffer = DataBuffer.getOrCreate(packetLength, false);
            // 返回的DatBuffer的Capacity可能大于packetLength，所以设置一下limit，不会多读
            dataBuffer.limit(packetLength);
            ByteBuffer buffer = dataBuffer.getBuffer();
            in.read(buffer.array(), buffer.arrayOffset(), packetLength);
            NioBuffer nioBuffer = new NioBuffer(dataBuffer, true); // 支持快速回收
            conn.handle(nioBuffer);
            dataBuffer.close();
        } catch (Exception e) {
            conn.handleException(e);
        }
    }
}

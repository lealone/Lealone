/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.bio;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;

import com.lealone.common.util.IOUtils;
import com.lealone.common.util.MapUtils;
import com.lealone.db.ConnectionSetting;
import com.lealone.db.DataBufferFactory;
import com.lealone.net.AsyncConnection;
import com.lealone.net.NetBuffer;
import com.lealone.net.WritableChannel;

public class BioWritableChannel implements WritableChannel {

    private final String host;
    private final int port;
    private final String localHost;
    private final int localPort;
    private final int maxPacketSize;

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    private AsyncConnection conn;

    public BioWritableChannel(Map<String, String> config, Socket socket, InetSocketAddress address)
            throws IOException {
        host = address.getHostString();
        port = address.getPort();
        localHost = socket.getLocalAddress().getHostAddress();
        localPort = socket.getLocalPort();
        maxPacketSize = getMaxPacketSize(config);
        this.socket = socket;
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024));
    }

    public void setAsyncConnection(AsyncConnection conn) {
        this.conn = conn;
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
    public String getLocalHost() {
        return localHost;
    }

    @Override
    public int getLocalPort() {
        return localPort;
    }

    @Override
    public boolean isBio() {
        return true;
    }

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return BioDataBufferFactory.INSTANCE;
    }

    @Override
    public void close() {
        if (socket != null) {
            IOUtils.closeSilently(in, out, socket);
            socket = null;
            in = null;
            out = null;
        }
    }

    @Override
    public void read() {
        try {
            if (conn.isClosed())
                return;
            int packetLength = in.readInt();
            checkPacketLength(maxPacketSize, packetLength);
            NetBuffer netBuffer = conn.getNetBuffer();
            // 如果返回的netBuffer的capacity小于packetLength会自动扩容，并且限制limit不会多读
            netBuffer.limit(packetLength);
            ByteBuffer buffer = netBuffer.getByteBuffer();
            // 要用readFully不能用read，因为read方法可能没有读够packetLength个字节，会导致后续解析失败
            in.readFully(buffer.array(), buffer.arrayOffset(), packetLength);
            conn.handle(netBuffer);
        } catch (Exception e) {
            conn.handleException(e);
        }
    }

    @Override
    public void write(NetBuffer data) {
        ByteBuffer bb = data.getByteBuffer();
        bb.flip();
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
            conn.handleException(e);
        } finally {
            data.reset();
        }
    }

    public static int getMaxPacketSize(Map<String, String> config) {
        return MapUtils.getInt(config, ConnectionSetting.MAX_PACKET_SIZE.name(), 16 * 1024 * 1024);
    }

    public static void checkPacketLength(int maxPacketSize, int packetLength) throws IOException {
        if (packetLength > maxPacketSize)
            throw new IOException(
                    "Packet too large, maxPacketSize: " + maxPacketSize + ", receive: " + packetLength);
    }
}

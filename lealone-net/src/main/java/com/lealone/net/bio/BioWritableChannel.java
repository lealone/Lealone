/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.bio;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    private InputStream in;
    private OutputStream out;

    private AsyncConnection conn;

    public BioWritableChannel(Map<String, String> config, Socket socket, InetSocketAddress address)
            throws IOException {
        host = address.getHostString();
        port = address.getPort();
        localHost = socket.getLocalAddress().getHostAddress();
        localPort = socket.getLocalPort();
        maxPacketSize = getMaxPacketSize(config);
        // 不需要套DataInputStream/BufferedInputStream和DataOutputStream/BufferedOutputStream
        // 上层已经有buffer了，直接用buffer读写原始的socket输入输出流即可
        in = socket.getInputStream();
        out = socket.getOutputStream();
        this.socket = socket;

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
            NetBuffer netBuffer = conn.getNetBuffer(); // 可能会变，所以每次都获取
            ByteBuffer bb = netBuffer.getByteBuffer();
            byte[] b = bb.array();
            int packetLength = readRacketLength(b);
            checkPacketLength(maxPacketSize, packetLength);
            // 如果返回的netBuffer的capacity小于packetLength会自动扩容，并且限制limit不会多读
            netBuffer.limit(packetLength);
            b = bb.array(); // 重新获取一次，扩容时会变
            // 要用readFully不能用read，因为read方法可能没有读够packetLength个字节，会导致后续解析失败
            readFully(b, 0, packetLength);
            conn.handle(netBuffer);
        } catch (Exception e) {
            conn.handleException(e);
        }
    }

    private int readRacketLength(byte[] b) throws IOException {
        readFully(b, 0, 4);
        return ((b[0] & 0xFF) << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8)
                + ((b[3] & 0xFF) << 0);
    }

    private void readFully(byte[] b, int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            int count = in.read(b, off + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        }
    }

    @Override
    public void write(NetBuffer data) {
        ByteBuffer bb = data.getByteBuffer();
        bb.flip(); // 上层没有进行过flip
        try {
            out.write(bb.array(), bb.arrayOffset(), bb.limit());
            out.flush();
        } catch (Exception e) {
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

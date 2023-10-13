/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.lealone.db.Constants;

public class NetNode implements Comparable<NetNode> {

    private static NetNode localTcpNode = new NetNode(Constants.DEFAULT_HOST,
            Constants.DEFAULT_TCP_PORT);
    private static NetNode localP2pNode = new NetNode(Constants.DEFAULT_HOST,
            Constants.DEFAULT_P2P_PORT);

    public static void setLocalTcpNode(String host, int port) {
        localTcpNode = new NetNode(host, port);
    }

    public static NetNode getLocalTcpNode() {
        return localTcpNode;
    }

    public static String getLocalTcpHostAndPort() {
        return localTcpNode.getHostAndPort();
    }

    public static void setLocalP2pNode(String host, int port) {
        localP2pNode = new NetNode(host, port);
    }

    public static NetNode getLocalP2pNode() {
        return localP2pNode;
    }

    private final InetAddress inetAddress;
    private final InetSocketAddress inetSocketAddress;
    private final String host;
    private final int port;
    private final String hostAndPort; // host + ":" + port;

    public NetNode(String host, int port) {
        this.host = host;
        this.port = port;
        inetSocketAddress = new InetSocketAddress(host, port);
        inetAddress = inetSocketAddress.getAddress();
        hostAndPort = host + ":" + port;
    }

    public NetNode(InetAddress inetAddress, int port) {
        this.inetAddress = inetAddress;
        this.host = inetAddress.getHostAddress();
        this.port = port;
        inetSocketAddress = new InetSocketAddress(host, port);
        hostAndPort = host + ":" + port;
    }

    public static NetNode getByName(String str) throws UnknownHostException {
        return createP2P(str);
    }

    public static NetNode createP2P(String str) {
        return new NetNode(str, true);
    }

    public static NetNode createTCP(String str) {
        return new NetNode(str, false);
    }

    public static NetNode create(String str, boolean p2p) {
        return new NetNode(str, p2p);
    }

    public NetNode(String str, boolean p2p) {
        int port = p2p ? Constants.DEFAULT_P2P_PORT : Constants.DEFAULT_TCP_PORT;
        // IPv6: RFC 2732 format is '[a:b:c:d:e:f:g:h]' or
        // '[a:b:c:d:e:f:g:h]:port'
        // RFC 2396 format is 'a.b.c.d' or 'a.b.c.d:port' or 'hostname' or
        // 'hostname:port'
        int startIndex = str.startsWith("[") ? str.indexOf(']') : 0;
        int idx = str.indexOf(':', startIndex);
        if (idx >= 0) {
            port = Integer.decode(str.substring(idx + 1));
            str = str.substring(0, idx);
        }
        this.host = str;
        this.port = port;
        this.inetSocketAddress = new InetSocketAddress(this.host, this.port);
        inetAddress = inetSocketAddress.getAddress();
        hostAndPort = host + ":" + port;
    }

    public String getHostAddress() {
        return inetAddress.getHostAddress(); // 不能用getHostName()，很慢
    }

    public String getHostAndPort() {
        return hostAndPort;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public byte[] getAddress() {
        return inetAddress.getAddress();
    }

    public InetAddress geInetAddress() {
        return inetAddress;
    }

    public InetSocketAddress getInetSocketAddress() {
        return inetSocketAddress;
    }

    @Override
    public String toString() {
        return "[host=" + host + ", port=" + port + "]";
    }

    private String tcpHostAndPort;

    public void setTcpHostAndPort(String tcpHostAndPort) {
        this.tcpHostAndPort = tcpHostAndPort;
    }

    public String getTcpHostAndPort() {
        return tcpHostAndPort;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((inetAddress == null) ? 0 : inetAddress.hashCode());
        result = prime * result + port;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        NetNode other = (NetNode) obj;
        if (inetAddress == null) {
            if (other.inetAddress != null)
                return false;
        } else if (!inetAddress.equals(other.inetAddress))
            return false;
        if (port != other.port)
            return false;
        return true;
    }

    @Override
    public int compareTo(NetNode o) {
        int v = this.getHostAddress().compareTo(o.getHostAddress());
        if (v == 0) {
            return this.port - o.port;
        }
        return v;
    }

    public void serialize(DataOutput out) throws IOException {
        byte[] bytes = getAddress(); // Inet4Address是4个字节，Inet6Address是16个字节
        out.writeByte(bytes.length);
        out.write(bytes);
        out.writeInt(getPort());
    }

    public static NetNode deserialize(DataInput in) throws IOException {
        byte[] bytes = new byte[in.readByte()];
        in.readFully(bytes, 0, bytes.length);
        int port = in.readInt();
        return new NetNode(InetAddress.getByAddress(bytes), port);
    }

    public static boolean isLocalTcpNode(String hostId) {
        return isLocalTcpNode(NetNode.createTCP(hostId));
    }

    public static boolean isLocalTcpNode(NetNode n) {
        return NetNode.getLocalTcpNode().equals(n);
    }

    public static boolean isLocalP2pNode(NetNode n) {
        return NetNode.getLocalP2pNode().equals(n);
    }
}

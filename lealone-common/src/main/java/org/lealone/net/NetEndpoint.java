/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.net;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.lealone.db.Constants;

public class NetEndpoint {

    private static NetEndpoint localTcpEndpoint = new NetEndpoint(Constants.DEFAULT_HOST, Constants.DEFAULT_TCP_PORT);
    private static NetEndpoint localP2pEndpoint = new NetEndpoint(Constants.DEFAULT_HOST, Constants.DEFAULT_P2P_PORT);

    public static void setLocalTcpEndpoint(String host, int port) {
        localTcpEndpoint = new NetEndpoint(host, port);
    }

    public static NetEndpoint getLocalTcpEndpoint() {
        return localTcpEndpoint;
    }

    public static void setLocalP2pEndpoint(String host, int port) {
        localP2pEndpoint = new NetEndpoint(host, port);
    }

    public static NetEndpoint getLocalP2pEndpoint() {
        return localP2pEndpoint;
    }

    private final InetAddress inetAddress;
    private InetSocketAddress inetSocketAddress;
    private String host;
    private int port;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((inetSocketAddress == null) ? 0 : inetSocketAddress.hashCode());
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
        NetEndpoint other = (NetEndpoint) obj;
        if (inetSocketAddress == null) {
            if (other.inetSocketAddress != null)
                return false;
        } else if (!inetSocketAddress.equals(other.inetSocketAddress))
            return false;
        return true;
    }

    public NetEndpoint(String host, int port) {
        this.host = host;
        this.port = port;
        inetSocketAddress = new InetSocketAddress(host, port);
        inetAddress = inetSocketAddress.getAddress();
    }

    public NetEndpoint(InetAddress inetAddress) {
        this.inetAddress = inetAddress;
    }

    public NetEndpoint(InetAddress inetAddress, int port) {
        this.inetAddress = inetAddress;
        this.host = inetAddress.getHostAddress();
        this.port = port;
        inetSocketAddress = new InetSocketAddress(host, port);
    }

    public static NetEndpoint getByName(String str) throws UnknownHostException {
        return createP2P(str);
    }

    public static NetEndpoint createP2P(String str) {
        return new NetEndpoint(str, true);
    }

    public static NetEndpoint createTCP(String str) {
        return new NetEndpoint(str, false);
    }

    public static NetEndpoint create(String str, boolean p2p) {
        return new NetEndpoint(str, p2p);
    }

    public NetEndpoint(String str, boolean p2p) {
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
    }

    public String getHostAddress() {
        return inetAddress.getHostAddress(); // 不能用getHostName()，很慢
    }

    public String getHostAndPort() {
        return host + ":" + port;
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

}

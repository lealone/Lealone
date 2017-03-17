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

import java.net.InetSocketAddress;

import org.lealone.db.Constants;

public class HostAndPort {

    private static HostAndPort local = new HostAndPort(Constants.DEFAULT_HOST, Constants.DEFAULT_TCP_PORT);

    public static void setLocalHostAndPort(String host, int port) {
        local = new HostAndPort(host, port);
    }

    public static HostAndPort getLocalHostAndPort() {
        return local;
    }

    public final String host;
    public final int port;

    public final String value; // host + ":" + port;
    public final InetSocketAddress inetSocketAddress;

    public HostAndPort(String str) {
        int port = Constants.DEFAULT_TCP_PORT;
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
        this.value = host + ":" + port;
        this.inetSocketAddress = new InetSocketAddress(this.host, this.port);
    }

    public HostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
        this.value = host + ":" + port;
        this.inetSocketAddress = new InetSocketAddress(this.host, this.port);
    }

    @Override
    public int hashCode() {
        return inetSocketAddress.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HostAndPort other = (HostAndPort) obj;
        return this.inetSocketAddress.equals(other.inetSocketAddress);
    }

    public boolean equals(String str) {
        return equals(new HostAndPort(str));
    }

}

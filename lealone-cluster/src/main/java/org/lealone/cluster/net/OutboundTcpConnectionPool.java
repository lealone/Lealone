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
package org.lealone.cluster.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.lealone.cluster.config.Config;
import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.db.ClusterMetaData;
import org.lealone.cluster.locator.IEndpointSnitch;
import org.lealone.cluster.metrics.ConnectionMetrics;
import org.lealone.cluster.security.SSLFactory;
import org.lealone.cluster.utils.Utils;

public class OutboundTcpConnectionPool {
    // pointer for the real Address.
    private final InetAddress id;
    private final CountDownLatch started;
    public final OutboundTcpConnection ackCon;
    // pointer to the reset Address.
    private InetAddress resetEndpoint;
    private ConnectionMetrics metrics;

    OutboundTcpConnectionPool(InetAddress remoteEp) {
        id = remoteEp;
        resetEndpoint = ClusterMetaData.getPreferredIP(remoteEp);
        started = new CountDownLatch(1);
        ackCon = new OutboundTcpConnection(this);
    }

    void start() {
        ackCon.start();
        metrics = new ConnectionMetrics(id);
        started.countDown();
    }

    void waitForStarted() {
        if (started.getCount() == 0)
            return;

        boolean error = false;
        try {
            if (!started.await(1, TimeUnit.MINUTES))
                error = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            error = true;
        }
        if (error)
            throw new IllegalStateException(String.format("Connections to %s are not started!", id.getHostAddress()));
    }

    void close() {
        // these null guards are simply for tests
        if (ackCon != null)
            ackCon.closeSocket(true);

        metrics.release();
    }

    OutboundTcpConnection getConnection() {
        return ackCon;
    }

    void reset() {
        ackCon.closeSocket(false);
    }

    /**
     * reconnect to @param remoteEP (after the current message backlog is exhausted).
     * Used by Ec2MultiRegionSnitch to force nodes in the same region to communicate over their private IPs.
     * @param remoteEP
     */
    public void reset(InetAddress remoteEP) {
        ClusterMetaData.updatePreferredIP(id, remoteEP);
        resetEndpoint = remoteEP;
        ackCon.softCloseSocket();

        // release previous metrics and create new one with reset address
        metrics.release();
        metrics = new ConnectionMetrics(resetEndpoint);
    }

    long getTimeouts() {
        return metrics.timeouts.count();
    }

    void incrementTimeout() {
        metrics.timeouts.mark();
    }

    Socket newSocket() throws IOException {
        return newSocket(endPoint());
    }

    public InetAddress endPoint() {
        if (id.equals(Utils.getBroadcastAddress()))
            return Utils.getLocalAddress();
        return resetEndpoint;
    }

    private static Socket newSocket(InetAddress endpoint) throws IOException {
        // zero means 'bind on any available port.'
        if (isEncryptedChannel(endpoint)) {
            if (Config.getOutboundBindAny())
                return SSLFactory.getSocket(DatabaseDescriptor.getServerEncryptionOptions(), endpoint,
                        DatabaseDescriptor.getSSLStoragePort());
            else
                return SSLFactory.getSocket(DatabaseDescriptor.getServerEncryptionOptions(), endpoint,
                        DatabaseDescriptor.getSSLStoragePort(), Utils.getLocalAddress(), 0);
        } else {
            Socket socket = SocketChannel.open(new InetSocketAddress(endpoint, DatabaseDescriptor.getStoragePort()))
                    .socket();
            if (Config.getOutboundBindAny() && !socket.isBound())
                socket.bind(new InetSocketAddress(Utils.getLocalAddress(), 0));
            return socket;
        }
    }

    private static boolean isEncryptedChannel(InetAddress address) {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        switch (DatabaseDescriptor.getServerEncryptionOptions().internode_encryption) {
        case none:
            return false; // if nothing needs to be encrypted then return immediately.
        case all:
            break;
        case dc:
            if (snitch.getDatacenter(address).equals(snitch.getDatacenter(Utils.getBroadcastAddress())))
                return false;
            break;
        case rack:
            // for rack then check if the DC's are the same.
            if (snitch.getRack(address).equals(snitch.getRack(Utils.getBroadcastAddress()))
                    && snitch.getDatacenter(address).equals(snitch.getDatacenter(Utils.getBroadcastAddress())))
                return false;
            break;
        }
        return true;
    }
}

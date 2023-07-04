/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.bio;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

import org.lealone.common.util.MapUtils;
import org.lealone.db.ConnectionSetting;
import org.lealone.db.Constants;
import org.lealone.db.async.AsyncCallback;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetClientBase;
import org.lealone.net.NetNode;
import org.lealone.net.TcpClientConnection;

class BioNetClient extends NetClientBase {

    BioNetClient() {
    }

    @Override
    protected void openInternal(Map<String, String> config) {
    }

    @Override
    protected void closeInternal() {
    }

    @Override
    protected void createConnectionInternal(Map<String, String> config, NetNode node,
            AsyncConnectionManager connectionManager, int maxSharedSize,
            AsyncCallback<AsyncConnection> ac) {
        InetSocketAddress inetSocketAddress = node.getInetSocketAddress();
        int socketRecvBuffer = MapUtils.getInt(config, "socket_recv_buffer_size", 16 * 1024);
        int socketSendBuffer = MapUtils.getInt(config, "socket_send_buffer_size", 8 * 1024);
        int networkTimeout = MapUtils.getInt(config, ConnectionSetting.NETWORK_TIMEOUT.name(),
                Constants.DEFAULT_NETWORK_TIMEOUT);
        Socket socket = null;
        try {
            socket = new Socket();
            socket.setSoTimeout(networkTimeout);
            socket.setReceiveBufferSize(socketRecvBuffer);
            socket.setSendBufferSize(socketSendBuffer);
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            socket.setReuseAddress(true);
            socket.connect(inetSocketAddress, networkTimeout);

            AsyncConnection conn;
            BioWritableChannel writableChannel = new BioWritableChannel(config, socket,
                    inetSocketAddress);
            if (connectionManager != null) {
                conn = connectionManager.createConnection(writableChannel, false);
            } else {
                conn = new TcpClientConnection(writableChannel, this, 1);
            }
            conn.setInetSocketAddress(inetSocketAddress);
            AsyncConnection conn2 = addConnection(inetSocketAddress, conn);
            ac.setAsyncResult(conn2);
        } catch (Exception e) {
            if (socket != null) {
                try {
                    socket.close();
                } catch (Throwable t) {
                }
            }
            ac.setAsyncResult(e);
        }
    }
}

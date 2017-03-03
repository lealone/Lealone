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
package org.lealone.aose.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;

import org.lealone.aose.concurrent.LealoneExecutorService;
import org.lealone.aose.concurrent.StageManager;
import org.lealone.aose.config.Config;
import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.metrics.ConnectionMetrics;
import org.lealone.aose.server.ClusterMetaData;
import org.lealone.aose.util.JVMStabilityInspector;
import org.lealone.aose.util.Utils;
import org.lealone.db.Session;
import org.lealone.net.AsyncCallback;
import org.lealone.net.AsyncConnection;
import org.lealone.net.Transfer;

import io.vertx.core.net.NetSocket;

public class TcpConnection extends AsyncConnection {

    private Transfer transfer;
    private DataOutputStream out;
    private InetAddress remoteEndpoint;
    private InetAddress resetEndpoint; // pointer to the reset Address.
    private String hostAndPort;
    private int targetVersion;
    private int version;

    private ConnectionMetrics metrics;

    public TcpConnection(NetSocket socket, boolean isServer) {
        super(socket, isServer);
    }

    String getHostId() {
        return hostAndPort;
    }

    long getTimeouts() {
        return metrics.timeouts.count();
    }

    void incrementTimeout() {
        metrics.timeouts.mark();
    }

    InetAddress endpoint() {
        if (remoteEndpoint.equals(Utils.getBroadcastAddress()))
            return Utils.getLocalAddress();
        return resetEndpoint;
    }

    @Override
    protected void close() {
        if (transfer != null)
            transfer.close();
        else if (socket != null) {
            socket.close();
        }
        super.close();
    }

    private void closeSocket(boolean destroyThread) {
        // backlog.clear();
        // isStopped = destroyThread; // Exit loop to stop the thread
        // enqueue(CLOSE_SENTINEL, -1);

        metrics.release();
    }

    void reset() {
        closeSocket(false);
    }

    void reset(InetAddress remoteEndpoint) {
        ClusterMetaData.updatePreferredIP(this.remoteEndpoint, remoteEndpoint);
        resetEndpoint = remoteEndpoint;
        // softCloseSocket();

        // release previous metrics and create new one with reset address
        metrics.release();
        metrics = new ConnectionMetrics(resetEndpoint);
    }

    void initTransfer(InetAddress remoteEndpoint, String hostAndPort) throws Exception {
        if (transfer == null) {
            this.remoteEndpoint = remoteEndpoint;
            resetEndpoint = ClusterMetaData.getPreferredIP(remoteEndpoint);
            metrics = new ConnectionMetrics(remoteEndpoint);
            this.hostAndPort = hostAndPort;
            transfer = new Transfer(this, socket, (Session) null);
            out = transfer.getDataOutputStream();
            targetVersion = MessagingService.instance().getVersion(remoteEndpoint);
            writeInitPacket(transfer, 0, targetVersion, shouldCompressConnection(), hostAndPort);
        }
    }

    private void writeInitPacket(Transfer transfer, int sessionId, int version, boolean compressionEnabled,
            String hostId) throws Exception {
        transfer.writeRequestHeader(sessionId, Session.SESSION_INIT);
        transfer.writeInt(MessagingService.PROTOCOL_MAGIC);
        transfer.writeInt(version);
        int header = 0;
        if (compressionEnabled)
            header |= 4;
        header |= (version << 8);
        transfer.writeInt(header);
        transfer.writeString(hostId);
        AsyncCallback<Void> ac = new AsyncCallback<>();
        transfer.addAsyncCallback(sessionId, ac);
        transfer.flush();
        ac.await();
    }

    private void readInitPacket(Transfer transfer, int sessionId) {
        try {
            MessagingService.validateMagic(transfer.readInt());
            version = transfer.readInt();
            transfer.readInt();
            hostAndPort = transfer.readString();
            transfer.writeResponseHeader(sessionId, Session.STATUS_OK);
            transfer.flush();
            // MessagingService.instance().addConnection(this);
        } catch (Throwable e) {
            sendError(transfer, sessionId, e);
        }
    }

    private boolean shouldCompressConnection() {
        return ConfigDescriptor.internodeCompression() == Config.InternodeCompression.all
                || (ConfigDescriptor.internodeCompression() == Config.InternodeCompression.dc
                        && !isLocalDC(remoteEndpoint));
    }

    private static boolean isLocalDC(InetAddress targetHost) {
        String remoteDC = ConfigDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = ConfigDescriptor.getEndpointSnitch().getDatacenter(Utils.getBroadcastAddress());
        return remoteDC.equals(localDC);
    }

    private void receiveMessage(DataInputStream in, int id) throws IOException {
        MessagingService.validateMagic(in.readInt());
        // int id = in.readInt();

        long timestamp = System.currentTimeMillis();
        // make sure to readInt, even if cross_node_to is not enabled
        int partial = in.readInt();
        if (ConfigDescriptor.hasCrossNodeTimeout())
            timestamp = (timestamp & 0xFFFFFFFF00000000L) | (((partial & 0xFFFFFFFFL) << 2) >> 2);

        MessageIn<?> message = MessageIn.read(in, version, id);
        if (message != null) {
            Runnable runnable = new MessageDeliveryTask(message, id, timestamp);
            LealoneExecutorService stage = StageManager.getStage(message.getMessageType());
            assert stage != null : "No stage for message type " + message.verb;
            stage.execute(runnable);
        }
        // message == null
        // callback expired; nothing to do
    }

    @Override
    protected void processRequest(Transfer transfer, int id, int operation) throws IOException {
        switch (operation) {
        case Session.SESSION_INIT: {
            readInitPacket(transfer, id);
            break;
        }
        case Session.COMMAND_STORAGE_MESSAGE: {
            receiveMessage(transfer.getDataInputStream(), id);
            break;
        }
        default:
            super.processRequest(transfer, id, operation);
        }
    }

    void enqueue(MessageOut<?> message, int id) {
        QueuedMessage qm = new QueuedMessage(message, id);
        try {
            sendMessage(qm.message, qm.id, qm.timestamp);
        } catch (IOException e) {
            JVMStabilityInspector.inspectThrowable(e);
        }
    }

    private void sendMessage(MessageOut<?> message, int id, long timestamp) throws IOException {
        transfer.writeRequestHeader(id, Session.COMMAND_STORAGE_MESSAGE);
        out.writeInt(MessagingService.PROTOCOL_MAGIC);

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        out.writeInt((int) timestamp);
        message.serialize(out, targetVersion);
        transfer.flush();
    }

    /** messages that have not been retried yet */
    static class QueuedMessage {
        final MessageOut<?> message;
        final int id;
        final long timestamp;
        final boolean droppable;

        QueuedMessage(MessageOut<?> message, int id) {
            this.message = message;
            this.id = id;
            this.timestamp = System.currentTimeMillis();
            this.droppable = MessagingService.DROPPABLE_VERBS.contains(message.verb);
        }

        /** don't drop a non-droppable message just because it's timestamp is expired */
        boolean isTimedOut(long maxTime) {
            return droppable && timestamp < System.currentTimeMillis() - maxTime;
        }

        boolean shouldRetry() {
            return !droppable;
        }

    }

    // TODO
    int getPendingMessages() {
        return 0;
    }

    // TODO
    long getCompletedMesssages() {
        return 0;
    }

    // TODO
    long getDroppedMessages() {
        return 0;
    }

}

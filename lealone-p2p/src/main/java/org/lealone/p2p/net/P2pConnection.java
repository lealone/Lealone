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
package org.lealone.p2p.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.JVMStabilityInspector;
import org.lealone.db.Session;
import org.lealone.db.async.AsyncTaskHandlerFactory;
import org.lealone.net.NetEndpoint;
import org.lealone.net.TransferConnection;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.net.WritableChannel;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.server.ClusterMetaData;

public class P2pConnection extends TransferConnection {

    private static final Logger logger = LoggerFactory.getLogger(P2pConnection.class);

    private String hostAndPort;
    private NetEndpoint remoteEndpoint;
    private NetEndpoint resetEndpoint; // pointer to the reset Address.
    // private ConnectionMetrics metrics;
    private int version;

    public P2pConnection(WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
    }

    public String getHostAndPort() {
        return hostAndPort;
    }

    @Override
    protected void handleRequest(TransferInputStream in, int packetId, int operation) throws IOException {
        switch (operation) {
        case Session.SESSION_INIT: {
            readInitPacket(in, packetId);
            break;
        }
        case Session.COMMAND_P2P_MESSAGE: {
            receiveMessage(in, packetId);
            break;
        }
        default:
            logger.warn("Unknow operation: {}", operation);
            close();
        }
    }

    synchronized void initTransfer(NetEndpoint remoteEndpoint, String localHostAndPort) throws Exception {
        if (this.remoteEndpoint == null) {
            this.remoteEndpoint = remoteEndpoint;
            resetEndpoint = ClusterMetaData.getPreferredIP(remoteEndpoint);
            // metrics = new ConnectionMetrics(remoteEndpoint);
            hostAndPort = remoteEndpoint.getHostAndPort();
            version = MessagingService.instance().getVersion(ConfigDescriptor.getLocalEndpoint());
            writeInitPacket(localHostAndPort);
            MessagingService.instance().addConnection(this);
        }
    }

    private void writeInitPacket(String localHostAndPort) throws Exception {
        int packetId = 0;
        TransferOutputStream transferOut = createTransferOutputStream(null);
        transferOut.writeRequestHeaderWithoutSessionId(packetId, Session.SESSION_INIT);
        transferOut.writeInt(MessagingService.PROTOCOL_MAGIC);
        transferOut.writeInt(version);
        transferOut.writeString(localHostAndPort);
        // AsyncCallback<Void> ac = new AsyncCallback<>();
        // transfer.addAsyncCallback(packetId, ac);
        transferOut.flush();
        // ac.await();
    }

    private void readInitPacket(TransferInputStream in, int packetId) {
        try {
            MessagingService.validateMagic(in.readInt());
            version = in.readInt();
            hostAndPort = in.readString();
            remoteEndpoint = NetEndpoint.createP2P(hostAndPort);
            resetEndpoint = ClusterMetaData.getPreferredIP(remoteEndpoint);
            // metrics = new ConnectionMetrics(remoteEndpoint);
            // transfer.writeResponseHeader(packetId, Session.STATUS_OK);
            // transfer.flush();
            MessagingService.instance().addConnection(this);
        } catch (Throwable e) {
            sendError(null, packetId, e);
        }
    }

    void enqueue(MessageOut<?> message, int id) {
        QueuedMessage qm = new QueuedMessage(message, id);
        try {
            sendMessage(message, id, qm.timestamp);
        } catch (IOException e) {
            JVMStabilityInspector.inspectThrowable(e);
        }
    }

    // 不需要加synchronized，因为会创建新的临时DataOutputStream
    private void sendMessage(MessageOut<?> message, int id, long timestamp) throws IOException {
        checkClosed();
        TransferOutputStream transferOut = createTransferOutputStream(null);
        DataOutputStream out = transferOut.getDataOutputStream();
        transferOut.writeRequestHeaderWithoutSessionId(id, Session.COMMAND_P2P_MESSAGE);
        out.writeInt(MessagingService.PROTOCOL_MAGIC);

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        out.writeInt((int) timestamp);
        message.serialize(transferOut, out, version);
        transferOut.flush();
    }

    private void receiveMessage(TransferInputStream transfer, int packetId) throws IOException {
        DataInputStream in = transfer.getDataInputStream();
        MessagingService.validateMagic(in.readInt());
        long timestamp = System.currentTimeMillis();
        // make sure to readInt, even if cross_node_to is not enabled
        int partial = in.readInt();
        if (ConfigDescriptor.hasCrossNodeTimeout())
            timestamp = (timestamp & 0xFFFFFFFF00000000L) | (((partial & 0xFFFFFFFFL) << 2) >> 2);

        MessageIn<?> message = MessageIn.read(in, version, packetId);
        if (message != null) {
            MessageDeliveryTask task = new MessageDeliveryTask(message, packetId, timestamp);
            AsyncTaskHandlerFactory.getAsyncTaskHandler().handle(task);
            // LealoneExecutorService stage = StageManager.getStage(message.getMessageType());
            // assert stage != null : "No stage for message type " + message.verb;
            // stage.execute(task);
        }
        // message == null
        // callback expired; nothing to do
    }

    long getTimeouts() {
        return 0; // metrics.timeouts.count();
    }

    void incrementTimeout() {
        // metrics.timeouts.mark();
    }

    NetEndpoint endpoint() {
        if (remoteEndpoint.equals(ConfigDescriptor.getLocalEndpoint()))
            return ConfigDescriptor.getLocalEndpoint();
        return resetEndpoint;
    }

    @Override
    public void close() {
        super.close();
        reset();
    }

    void reset() {
        // metrics.release();
    }

    void reset(NetEndpoint remoteEndpoint) {
        ClusterMetaData.updatePreferredIP(this.remoteEndpoint, remoteEndpoint);
        resetEndpoint = remoteEndpoint;

        // release previous metrics and create new one with reset address
        // metrics.release();
        // metrics = new ConnectionMetrics(resetEndpoint);
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

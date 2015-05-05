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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Checksum;

import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import org.lealone.cluster.config.Config;
import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.db.ClusterMetaData;
import org.lealone.cluster.io.DataOutputStreamPlus;
import org.lealone.cluster.locator.IEndpointSnitch;
import org.lealone.cluster.metrics.ConnectionMetrics;
import org.lealone.cluster.security.SSLFactory;
import org.lealone.cluster.utils.JVMStabilityInspector;
import org.lealone.cluster.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Uninterruptibles;

class OutboundTcpConnection extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnection.class);

    private static final MessageOut<Void> CLOSE_SENTINEL = new MessageOut<>(MessagingService.Verb.INTERNAL_RESPONSE);

    static final int LZ4_HASH_SEED = 0x9747b28c;
    static final int WAIT_FOR_VERSION_MAX_TIME = 5000;

    private static final int OPEN_RETRY_DELAY = 100; // ms between retries

    private static boolean isLocalDC(InetAddress targetHost) {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(Utils.getBroadcastAddress());
        return remoteDC.equals(localDC);
    }

    private final BlockingQueue<QueuedMessage> backlog = new LinkedBlockingQueue<>();
    private final InetAddress remoteEndpoint;
    private final AtomicLong dropped = new AtomicLong();

    // pointer to the reset Address.
    private InetAddress resetEndpoint;

    private DataOutputStreamPlus out;
    private Socket socket;

    private volatile long completed;
    private volatile int currentMsgBufferCount = 0;
    private volatile boolean isStopped = false;

    private int targetVersion;
    private ConnectionMetrics metrics;

    OutboundTcpConnection(InetAddress remoteEndpoint) {
        super("OutboundTcpConnection-" + remoteEndpoint);
        this.remoteEndpoint = remoteEndpoint;
        resetEndpoint = ClusterMetaData.getPreferredIP(remoteEndpoint);
        metrics = new ConnectionMetrics(remoteEndpoint);
    }

    @Override
    public void run() {
        // keeping list (batch) size small for now; that way we don't have an unbounded array (that we never resize)
        final List<QueuedMessage> drainedMessages = new ArrayList<>(128);
        outer: while (true) {
            if (backlog.drainTo(drainedMessages, drainedMessages.size()) == 0) {
                try {
                    drainedMessages.add(backlog.take());
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }

            }
            currentMsgBufferCount = drainedMessages.size();

            int count = drainedMessages.size();
            for (QueuedMessage qm : drainedMessages) {
                try {
                    MessageOut<?> m = qm.message;
                    if (m == CLOSE_SENTINEL) {
                        disconnect();
                        if (isStopped)
                            break outer;
                        continue;
                    }
                    if (qm.isTimedOut(m.getTimeout()))
                        dropped.incrementAndGet();
                    else if (socket != null || connect())
                        writeConnected(qm, count == 1 && backlog.size() == 0);
                    else
                        // clear out the queue, else gossip messages back up.
                        backlog.clear();
                } catch (Exception e) {
                    JVMStabilityInspector.inspectThrowable(e);
                    // really shouldn't get here, as exception handling in writeConnected() is reasonably robust
                    // but we want to catch anything bad we don't drop the messages in the current batch
                    logger.error("error processing a message intended for {}", remoteEndpoint, e);
                }
                currentMsgBufferCount = --count;
            }
            drainedMessages.clear();
        }
    }

    void enqueue(MessageOut<?> message, int id) {
        if (backlog.size() > 1024)
            expireMessages();
        try {
            backlog.put(new QueuedMessage(message, id));
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    int getPendingMessages() {
        return backlog.size() + currentMsgBufferCount;
    }

    long getCompletedMesssages() {
        return completed;
    }

    long getDroppedMessages() {
        return dropped.get();
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

    void close() {
        closeSocket(true);
    }

    void reset() {
        closeSocket(false);
    }

    void reset(InetAddress remoteEndpoint) {
        ClusterMetaData.updatePreferredIP(this.remoteEndpoint, remoteEndpoint);
        resetEndpoint = remoteEndpoint;
        softCloseSocket();

        // release previous metrics and create new one with reset address
        metrics.release();
        metrics = new ConnectionMetrics(resetEndpoint);
    }

    private void softCloseSocket() {
        enqueue(CLOSE_SENTINEL, -1);
    }

    private void closeSocket(boolean destroyThread) {
        backlog.clear();
        isStopped = destroyThread; // Exit loop to stop the thread
        enqueue(CLOSE_SENTINEL, -1);

        metrics.release();
    }

    private boolean shouldCompressConnection() {
        return DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all
                || (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc && !isLocalDC(remoteEndpoint));
    }

    private void writeConnected(QueuedMessage qm, boolean flush) {
        try {
            sendMessage(qm.message, qm.id, qm.timestamp);

            completed++;
            if (flush)
                out.flush();
        } catch (Exception e) {
            disconnect();
            if (e instanceof IOException) {
                if (logger.isDebugEnabled())
                    logger.debug("error writing to {}", remoteEndpoint, e);

                // if the message was important, such as a repair acknowledgement, put it back on the queue
                // to retry after re-connecting.  See lealone-5393
                if (qm.shouldRetry()) {
                    try {
                        backlog.put(new RetriedQueuedMessage(qm));
                    } catch (InterruptedException e1) {
                        throw new AssertionError(e1);
                    }
                }
            } else {
                // Non IO exceptions are likely a programming error so let's not silence them
                logger.error("error writing to {}", remoteEndpoint, e);
            }
        }
    }

    private void sendMessage(MessageOut<?> message, int id, long timestamp) throws IOException {
        out.writeInt(MessagingService.PROTOCOL_MAGIC);
        out.writeInt(id);

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        out.writeInt((int) timestamp);
        message.serialize(out, targetVersion);
    }

    private void disconnect() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                if (logger.isTraceEnabled())
                    logger.trace("exception closing connection to " + remoteEndpoint, e);
            }
            out = null;
            socket = null;
        }
    }

    private boolean connect() {
        if (logger.isDebugEnabled())
            logger.debug("attempting to connect to {}", remoteEndpoint);

        long start = System.nanoTime();
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
        while (System.nanoTime() - start < timeout) {
            targetVersion = MessagingService.instance().getVersion(remoteEndpoint);
            try {
                socket = newSocket();
                socket.setKeepAlive(true);
                if (isLocalDC(remoteEndpoint)) {
                    socket.setTcpNoDelay(true);
                } else {
                    socket.setTcpNoDelay(DatabaseDescriptor.getInterDCTcpNoDelay());
                }
                if (DatabaseDescriptor.getInternodeSendBufferSize() != null) {
                    try {
                        socket.setSendBufferSize(DatabaseDescriptor.getInternodeSendBufferSize());
                    } catch (SocketException se) {
                        logger.warn("Failed to set send buffer size on internode socket.", se);
                    }
                }
                out = new DataOutputStreamPlus(new BufferedOutputStream(socket.getOutputStream(), 4096));

                //write header
                out.writeInt(MessagingService.PROTOCOL_MAGIC);
                out.writeInt(targetVersion);
                out.writeBoolean(shouldCompressConnection());
                CompactEndpointSerializationHelper.serialize(Utils.getBroadcastAddress(), out);
                out.flush();

                DataInputStream in = new DataInputStream(socket.getInputStream());
                int maxTargetVersion = in.readInt();
                MessagingService.instance().setVersion(remoteEndpoint, maxTargetVersion);

                if (targetVersion > maxTargetVersion) {
                    if (logger.isDebugEnabled())
                        logger.debug("Target max version is {}; will reconnect with that version", maxTargetVersion);
                    disconnect();
                    return false;
                }

                if (targetVersion < maxTargetVersion && targetVersion < MessagingService.CURRENT_VERSION) {
                    if (logger.isTraceEnabled())
                        logger.trace(
                                "Detected higher max version {} (using {}); will reconnect when queued messages are done",
                                maxTargetVersion, targetVersion);
                    softCloseSocket();
                }

                if (shouldCompressConnection()) {
                    if (logger.isTraceEnabled())
                        logger.trace("Upgrading OutputStream to be compressed");
                    // TODO: custom LZ4 OS that supports BB write methods
                    LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
                    Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(LZ4_HASH_SEED).asChecksum();

                    // 16k block size
                    out = new DataOutputStreamPlus(new LZ4BlockOutputStream(socket.getOutputStream(), 1 << 14,
                            compressor, checksum, true)); // no async flushing
                }

                return true;
            } catch (IOException e) {
                socket = null;
                if (logger.isTraceEnabled())
                    logger.trace("unable to connect to " + remoteEndpoint, e);
                Uninterruptibles.sleepUninterruptibly(OPEN_RETRY_DELAY, TimeUnit.MILLISECONDS);
            }
        }
        return false;
    }

    private void expireMessages() {
        Iterator<QueuedMessage> iter = backlog.iterator();
        while (iter.hasNext()) {
            QueuedMessage qm = iter.next();
            if (qm.timestamp >= System.currentTimeMillis() - qm.message.getTimeout())
                return;
            iter.remove();
            dropped.incrementAndGet();
        }
    }

    private Socket newSocket() throws IOException {
        return newSocket(endpoint());
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

    /** messages that have not been retried yet */
    private static class QueuedMessage {
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

    private static class RetriedQueuedMessage extends QueuedMessage {
        RetriedQueuedMessage(QueuedMessage msg) {
            super(msg.message, msg.id);
        }

        @Override
        boolean shouldRetry() {
            return false;
        }
    }
}

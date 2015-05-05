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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.zip.Checksum;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.xxhash.XXHashFactory;

import org.lealone.cluster.concurrent.LealoneExecutorService;
import org.lealone.cluster.concurrent.StageManager;
import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.gms.Gossiper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IncomingTcpConnection extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(IncomingTcpConnection.class);

    private final int version;
    private final boolean compressed;
    private final InetAddress from;
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    IncomingTcpConnection(Socket socket) throws IOException {
        super("IncomingTcpConnection-" + socket.getInetAddress());

        this.socket = socket;

        socket.setKeepAlive(true);
        socket.setSoTimeout(2 * OutboundTcpConnection.WAIT_FOR_VERSION_MAX_TIME);
        // determine the connection type to decide whether to buffer
        DataInputStream in = new DataInputStream(socket.getInputStream());

        //read header
        MessagingService.validateMagic(in.readInt());
        version = in.readInt();
        compressed = in.readBoolean();
        from = CompactEndpointSerializationHelper.deserialize(in);

        if (logger.isDebugEnabled())
            logger.debug("Connection version {} from {}", version, socket.getInetAddress());
        socket.setSoTimeout(0);

        MessagingService.instance().setVersion(from, version);

        out = new DataOutputStream(socket.getOutputStream());
        out.writeInt(MessagingService.CURRENT_VERSION);
        out.flush();

        if (compressed) {
            if (logger.isDebugEnabled())
                logger.debug("Upgrading incoming connection to be compressed");
            LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();
            Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(OutboundTcpConnection.LZ4_HASH_SEED)
                    .asChecksum();
            this.in = new DataInputStream(new LZ4BlockInputStream(socket.getInputStream(), decompressor, checksum));
        } else {
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 4096));
        }

        if (DatabaseDescriptor.getInternodeRecvBufferSize() != null) {
            try {
                this.socket.setReceiveBufferSize(DatabaseDescriptor.getInternodeRecvBufferSize());
            } catch (SocketException se) {
                logger.warn("Failed to set receive buffer size on internode socket.", se);
            }
        }
    }

    /**
     * A new connection will either stream or message for its entire lifetime: because streaming
     * bypasses the InputStream implementations to use sendFile, we cannot begin buffering until
     * we've determined the type of the connection.
     */
    @Override
    public void run() {
        try {
            receiveMessages();
        } catch (EOFException e) {
            if (logger.isTraceEnabled())
                logger.trace("eof reading from socket; closing", e);
            // connection will be reset so no need to throw an exception.
        } catch (IOException e) {
            if (logger.isDebugEnabled())
                logger.debug("IOException reading from socket; closing", e);
        } finally {
            close();
        }
    }

    private void close() {
        try {
            socket.close();
        } catch (IOException e) {
            if (logger.isDebugEnabled())
                logger.debug("Error closing socket", e);
        }
    }

    private void receiveMessages() throws IOException {
        if (version > MessagingService.CURRENT_VERSION) {
            // save the endpoint so gossip will reconnect to it
            Gossiper.instance.addSavedEndpoint(from);
            logger.info("Received messages from newer protocol version {}. Ignoring", version);
            return;
        }

        while (true) {
            receiveMessage();
        }
    }

    //对应OutboundTcpConnection.sendMessage
    private void receiveMessage() throws IOException {
        MessagingService.validateMagic(in.readInt());
        int id = in.readInt();

        long timestamp = System.currentTimeMillis();
        // make sure to readInt, even if cross_node_to is not enabled
        int partial = in.readInt();
        if (DatabaseDescriptor.hasCrossNodeTimeout())
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
}

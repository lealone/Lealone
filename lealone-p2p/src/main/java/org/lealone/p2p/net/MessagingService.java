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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.lealone.common.concurrent.ScheduledExecutors;
import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.net.NetEndpoint;
import org.lealone.net.NetFactory;
import org.lealone.p2p.concurrent.Stage;
import org.lealone.p2p.concurrent.StageManager;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.gms.EchoMessage;
import org.lealone.p2p.gms.EchoVerbHandler;
import org.lealone.p2p.gms.GossipDigestAck;
import org.lealone.p2p.gms.GossipDigestAck2;
import org.lealone.p2p.gms.GossipDigestAck2VerbHandler;
import org.lealone.p2p.gms.GossipDigestAckVerbHandler;
import org.lealone.p2p.gms.GossipDigestSyn;
import org.lealone.p2p.gms.GossipDigestSynVerbHandler;
import org.lealone.p2p.gms.GossipShutdownVerbHandler;
import org.lealone.p2p.gms.Gossiper;
import org.lealone.p2p.locator.ILatencySubscriber;
import org.lealone.p2p.metrics.ConnectionMetrics;
import org.lealone.p2p.metrics.DroppedMessageMetrics;
import org.lealone.p2p.server.ClusterMetaData;
import org.lealone.p2p.server.P2pServer;
import org.lealone.p2p.util.ExpiringMap;
import org.lealone.p2p.util.Pair;
import org.lealone.p2p.util.Utils;

import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;

@SuppressWarnings({ "rawtypes" })
public final class MessagingService implements MessagingServiceMBean {

    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);

    /* All verb handler identifiers */
    public static enum Verb {
        REQUEST_RESPONSE, // client-initiated reads and writes
        GOSSIP_DIGEST_SYN,
        GOSSIP_DIGEST_ACK,
        GOSSIP_DIGEST_ACK2,
        GOSSIP_SHUTDOWN,
        INTERNAL_RESPONSE, // responses to internal calls
        ECHO,
        // remember to add new verbs at the end, since we serialize by ordinal
        UNUSED_1,
        UNUSED_2,
        UNUSED_3;
    }

    public static final int VERSION_10 = 1;
    public static final int CURRENT_VERSION = VERSION_10;

    public static final String FAILURE_CALLBACK_PARAM = "CAL_BAC";
    public static final String FAILURE_RESPONSE_PARAM = "FAIL";
    public static final byte[] ONE_BYTE = new byte[1];

    /**
     * we preface every message with this number so the recipient can validate the sender is sane
     */
    public static final int PROTOCOL_MAGIC = 0xCA552DFA;

    public static void validateMagic(int magic) throws IOException {
        if (magic != PROTOCOL_MAGIC)
            throw new IOException("invalid protocol header");
    }

    /**
     * Verbs it's okay to drop if the request has been queued longer than the request timeout.
     * These all correspond to client requests or something triggered by them; 
     * we don't want to drop internal messages like bootstrap.
     */
    public static final EnumSet<Verb> DROPPABLE_VERBS = EnumSet.of(Verb.REQUEST_RESPONSE);

    private static final int LOG_DROPPED_INTERVAL_IN_MS = 5000;

    public static final EnumMap<MessagingService.Verb, Stage> verbStages = new EnumMap<MessagingService.Verb, Stage>(
            MessagingService.Verb.class) {
        {
            put(Verb.REQUEST_RESPONSE, Stage.REQUEST_RESPONSE);
            put(Verb.INTERNAL_RESPONSE, Stage.INTERNAL_RESPONSE);

            put(Verb.GOSSIP_DIGEST_ACK, Stage.GOSSIP);
            put(Verb.GOSSIP_DIGEST_ACK2, Stage.GOSSIP);
            put(Verb.GOSSIP_DIGEST_SYN, Stage.GOSSIP);
            put(Verb.GOSSIP_SHUTDOWN, Stage.GOSSIP);
            put(Verb.ECHO, Stage.GOSSIP);

            put(Verb.UNUSED_1, Stage.INTERNAL_RESPONSE);
            put(Verb.UNUSED_2, Stage.INTERNAL_RESPONSE);
            put(Verb.UNUSED_3, Stage.INTERNAL_RESPONSE);
        }
    };

    /**
     * Messages we receive in IncomingTcpConnection have a Verb that tells us what kind of message it is.
     * Most of the time, this is enough to determine how to deserialize the message payload.
     * The exception is the REQUEST_RESPONSE verb, which just means "a reply to something you told me to do."
     * Traditionally, this was fine since each VerbHandler knew what type of payload it expected, and
     * handled the deserialization itself.  Now that we do that in ITC, to avoid the extra copy to an
     * intermediary byte[] (See lealone-3716), we need to wire that up to the CallbackInfo object
     * (see below).
     */
    public static final EnumMap<Verb, IVersionedSerializer<?>> verbSerializers = new EnumMap<Verb, IVersionedSerializer<?>>(
            Verb.class) {
        {
            put(Verb.REQUEST_RESPONSE, CallbackDeterminedSerializer.instance);
            put(Verb.INTERNAL_RESPONSE, CallbackDeterminedSerializer.instance);
            put(Verb.GOSSIP_DIGEST_ACK, GossipDigestAck.serializer);
            put(Verb.GOSSIP_DIGEST_ACK2, GossipDigestAck2.serializer);
            put(Verb.GOSSIP_DIGEST_SYN, GossipDigestSyn.serializer);
            put(Verb.ECHO, EchoMessage.serializer);
        }
    };

    /**
     * A Map of what kind of serializer to wire up to a REQUEST_RESPONSE callback, based on outbound Verb.
     */
    private static final EnumMap<Verb, IVersionedSerializer<?>> callbackDeserializers = new EnumMap<>(Verb.class);

    private static final AtomicInteger idGen = new AtomicInteger(0);

    private static int nextId() {
        return idGen.incrementAndGet();
    }

    /**
     * a placeholder class that means "deserialize using the callback." We can't implement this without
     * special-case code in InboundTcpConnection because there is no way to pass the message id to IVersionedSerializer.
     */
    static class CallbackDeterminedSerializer implements IVersionedSerializer<Object> {
        public static final CallbackDeterminedSerializer instance = new CallbackDeterminedSerializer();

        @Override
        public Object deserialize(DataInput in, int version) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void serialize(Object o, DataOutput out, int version) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long serializedSize(Object o, int version) {
            throw new UnsupportedOperationException();
        }
    }

    /* This records all the results mapped by message Id */
    private final ExpiringMap<Integer, CallbackInfo> callbacks;

    /* Lookup table for registering message handlers based on the verb. */
    private final Map<Verb, IVerbHandler> verbHandlers = new EnumMap<>(Verb.class);;

    private final ConcurrentMap<NetEndpoint, TcpConnection> connectionManagers = new ConcurrentHashMap<>();

    // total dropped message counts for server lifetime
    private final Map<Verb, DroppedMessageMetrics> droppedMessages = new EnumMap<>(Verb.class);
    // dropped count when last requested for the Recent api. high concurrency isn't necessary here.
    private final Map<Verb, Integer> lastDroppedInternal = new EnumMap<>(Verb.class);

    private final List<ILatencySubscriber> subscribers = new ArrayList<>();

    // protocol versions of the other nodes in the cluster
    private final ConcurrentMap<NetEndpoint, Integer> versions = new ConcurrentHashMap<>();

    private static class MSHandle {
        public static final MessagingService instance = new MessagingService();
    }

    public static MessagingService instance() {
        return MSHandle.instance;
    }

    private void registerDefaultVerbHandlers() {
        registerVerbHandler(Verb.REQUEST_RESPONSE, new ResponseVerbHandler());
        registerVerbHandler(Verb.INTERNAL_RESPONSE, new ResponseVerbHandler());
        registerVerbHandler(Verb.GOSSIP_SHUTDOWN, new GossipShutdownVerbHandler());
        registerVerbHandler(Verb.GOSSIP_DIGEST_SYN, new GossipDigestSynVerbHandler());
        registerVerbHandler(Verb.GOSSIP_DIGEST_ACK, new GossipDigestAckVerbHandler());
        registerVerbHandler(Verb.GOSSIP_DIGEST_ACK2, new GossipDigestAck2VerbHandler());
        registerVerbHandler(Verb.ECHO, new EchoVerbHandler());
    }

    private MessagingService() {
        registerDefaultVerbHandlers();

        for (Verb verb : DROPPABLE_VERBS) {
            droppedMessages.put(verb, new DroppedMessageMetrics(verb));
            lastDroppedInternal.put(verb, 0);
        }

        Runnable logDropped = new Runnable() {
            @Override
            public void run() {
                logDroppedMessages();
            }
        };
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(logDropped, LOG_DROPPED_INTERVAL_IN_MS,
                LOG_DROPPED_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);

        Function<Pair<Integer, ExpiringMap.CacheableObject<CallbackInfo>>, ?> timeoutReporter = //
                new Function<Pair<Integer, ExpiringMap.CacheableObject<CallbackInfo>>, Object>() {
                    @Override
                    public Object apply(Pair<Integer, ExpiringMap.CacheableObject<CallbackInfo>> pair) {
                        final CallbackInfo expiredCallbackInfo = pair.right.value;
                        maybeAddLatency(expiredCallbackInfo.callback, expiredCallbackInfo.target, pair.right.timeout);
                        ConnectionMetrics.totalTimeouts.mark();
                        getConnection(expiredCallbackInfo.target).incrementTimeout();
                        if (expiredCallbackInfo.isFailureCallback()) {
                            StageManager.getStage(Stage.INTERNAL_RESPONSE).submit(new Runnable() {
                                @Override
                                public void run() {
                                    ((IAsyncCallbackWithFailure) expiredCallbackInfo.callback)
                                            .onFailure(expiredCallbackInfo.target);
                                }
                            });
                        }
                        return null;
                    }
                };

        callbacks = new ExpiringMap<Integer, CallbackInfo>(ConfigDescriptor.getRpcTimeout(), timeoutReporter);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(this, new ObjectName(Utils.getJmxObjectName("MessagingService")));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void incrementDroppedMessages(Verb verb) {
        assert DROPPABLE_VERBS.contains(verb) : "Verb " + verb + " should not legally be dropped";
        droppedMessages.get(verb).dropped.mark();
    }

    private void logDroppedMessages() {
        for (Map.Entry<Verb, DroppedMessageMetrics> entry : droppedMessages.entrySet()) {
            int dropped = (int) entry.getValue().dropped.count();
            Verb verb = entry.getKey();
            int recent = dropped - lastDroppedInternal.get(verb);
            if (recent > 0) {
                logger.info("{} {} messages dropped in last {}ms",
                        new Object[] { recent, verb, LOG_DROPPED_INTERVAL_IN_MS });
                lastDroppedInternal.put(verb, dropped);
            }
        }
    }

    /**
     * Track latency information for the dynamic snitch
     * 
     * @param cb      the callback associated with this message -- 
     *                this lets us know if it's a message type we're interested in
     * @param address the host that replied to the message
     * @param latency
     */
    public void maybeAddLatency(IAsyncCallback cb, NetEndpoint address, long latency) {
        if (cb.isLatencyForSnitch())
            addLatency(address, latency);
    }

    public void addLatency(NetEndpoint address, long latency) {
        for (ILatencySubscriber subscriber : subscribers)
            subscriber.receiveTiming(address, latency);
    }

    private Server server;

    /**
     * Listen on the specified port.
     *
     * @param localEp NetEndpoint whose port to listen on.
     */
    public void start(NetEndpoint localEp, Map<String, String> config) throws ConfigException {
        initVertx(config);
        callbacks.reset(); // hack to allow tests to stop/restart MS
        server = new Server(getNetServer(localEp));
        server.start();
    }

    private NetServer getNetServer(NetEndpoint localEp) throws ConfigException {
        boolean ssl = P2pServer.instance.isSSL();
        int port = localEp.getPort();
        NetServerOptions nso;
        if (ssl) {
            ServerEncryptionOptions options = ConfigDescriptor.getServerEncryptionOptions();
            nso = NetFactory.getNetServerOptions(options);
            logger.info("Starting Encrypted Messaging Service on port {}", port);
        } else {
            nso = NetFactory.getNetServerOptions(null);
            logger.info("Starting Messaging Service on port {}", port);
        }
        nso.setHost(localEp.getHost());
        nso.setPort(port);
        NetServer server = vertx.createNetServer(nso);
        return server;
    }

    private static Vertx vertx;
    private static NetClient client;

    private synchronized static void initVertx(Map<String, String> config) {
        if (vertx == null) {
            vertx = NetFactory.getVertx(config);
            NetClientOptions options = NetFactory.getNetClientOptions(ConfigDescriptor.getClientEncryptionOptions());
            options.setConnectTimeout(10000);
            options.setReconnectAttempts(3);
            client = vertx.createNetClient(options);
        }
    }

    private final ConcurrentHashMap<String, TcpConnection> asyncConnections = new ConcurrentHashMap<>();

    void addConnection(TcpConnection conn) {
        TcpConnection oldConn = asyncConnections.put(conn.getHostId(), conn);
        if (oldConn != null) {
            oldConn.close();
        }
    }

    private static class Server {
        private final NetServer netServer;
        // private final Set<Closeable> connections = Sets.newConcurrentHashSet();

        Server(NetServer netServer) {
            this.netServer = netServer;
        }

        public void start() {
            netServer.connectHandler(socket -> {
                if (Server.this.authenticate(socket)) {
                    TcpConnection c = new TcpConnection(socket, true);
                    socket.handler(c);
                } else {
                    logger.trace("remote failed to authenticate");
                    socket.close();
                    // throw DbException.get(ErrorCode.REMOTE_CONNECTION_NOT_ALLOWED);
                }
            });

            CountDownLatch latch = new CountDownLatch(1);
            netServer.listen(res -> {
                latch.countDown();
                if (res.succeeded()) {
                    logger.info("MessagingService listening on port " + netServer.actualPort());
                } else {
                    Throwable e = res.cause();
                    String address = ConfigDescriptor.getLocalEndpoint().getHostAddress();
                    if (e instanceof BindException) {
                        if (e.getMessage().contains("in use"))
                            throw new ConfigException(address + " is in use by another process.  "
                                    + "Change listen_address:storage_port in lealone.yaml "
                                    + "to values that do not conflict with other services");
                        else if (e.getMessage().contains("Cannot assign requested address"))
                            throw new ConfigException("Unable to bind to address " + address
                                    + ". Set listen_address in lealone.yaml to an interface you can bind to, e.g.,"
                                    + " your private IP address on EC2");
                    }
                    throw DbException.convert(e);
                }
            });
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw DbException.convert(e);
            }
        }

        void close() throws IOException {
            logger.trace("Closing accept() server");
            // for (Closeable connection : connections) {
            // connection.close();
            // }
            netServer.close();
        }

        // TODO
        private boolean authenticate(NetSocket socket) {
            return true;
            // return ConfigDescriptor.getInternodeAuthenticator().authenticate(socket.remoteAddress(), 990);
        }
    }

    /**
     * Register a verb and the corresponding verb handler with the
     * Messaging Service.
     *
     * @param verb
     * @param verbHandler handler for the specified verb
     */
    public void registerVerbHandler(Verb verb, IVerbHandler verbHandler) {
        assert !verbHandlers.containsKey(verb);
        verbHandlers.put(verb, verbHandler);
    }

    /**
     * This method returns the verb handler associated with the registered
     * verb. If no handler has been registered then null is returned.
     *
     * @param type for which the verb handler is sought
     * @return a reference to IVerbHandler which is the handler for the specified verb
     */
    public IVerbHandler getVerbHandler(Verb type) {
        return verbHandlers.get(type);
    }

    public int sendRR(MessageOut message, NetEndpoint to, IAsyncCallback cb) {
        return sendRR(message, to, cb, message.getTimeout(), false);
    }

    public int sendRRWithFailure(MessageOut message, NetEndpoint to, IAsyncCallbackWithFailure cb) {
        return sendRR(message, to, cb, message.getTimeout(), true);
    }

    /**
     * Send a non-mutation message to a given endpoint. This method specifies a callback
     * which is invoked with the actual response.
     *
     * @param message message to be sent.
     * @param to      endpoint to which the message needs to be sent
     * @param cb      callback interface which is used to pass the responses or
     *                suggest that a timeout occurred to the invoker of the send().
     * @param timeout the timeout used for expiration
     * @return an reference to message id used to match with the result
     */
    public int sendRR(MessageOut message, NetEndpoint to, IAsyncCallback cb, long timeout, boolean failureCallback) {
        int id = addCallback(message, to, cb, timeout, failureCallback);
        sendOneWay(failureCallback ? message.withParameter(FAILURE_CALLBACK_PARAM, ONE_BYTE) : message, id, to);
        return id;
    }

    private int addCallback(MessageOut message, NetEndpoint to, IAsyncCallback cb, long timeout,
            boolean failureCallback) {
        int messageId = nextId();
        CallbackInfo previous = callbacks.put(messageId,
                new CallbackInfo(to, cb, callbackDeserializers.get(message.verb), failureCallback), timeout);
        assert previous == null : String.format("Callback already exists for id %d! (%s)", messageId, previous);
        return messageId;
    }

    public void sendOneWay(MessageOut message, NetEndpoint to) {
        sendOneWay(message, nextId(), to);
    }

    public void sendReply(MessageOut message, int id, NetEndpoint to) {
        sendOneWay(message, id, to);
    }

    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     *
     * @param message messages to be sent.
     * @param to      endpoint to which the message needs to be sent
     */
    public void sendOneWay(MessageOut message, int id, NetEndpoint to) {
        if (logger.isTraceEnabled()) {
            if (to.equals(ConfigDescriptor.getLocalEndpoint()))
                logger.trace("Message-to-self {} going over MessagingService", message);
            else
                logger.trace("{} sending {} to {}@{}", ConfigDescriptor.getLocalEndpoint(), message.verb, id, to);
        }

        TcpConnection conn = getConnection(to);
        if (conn != null)
            conn.enqueue(message, id);
    }

    public TcpConnection getConnection(NetEndpoint remoteEndpoint) {
        NetEndpoint resetEndpoint = ClusterMetaData.getPreferredIP(remoteEndpoint);
        final String remoteHostAndPort = resetEndpoint.getHostAndPort();
        TcpConnection asyncConnection = asyncConnections.get(remoteHostAndPort);
        if (asyncConnection == null) {
            final int port = resetEndpoint.getPort();
            final String host = resetEndpoint.getHost();
            synchronized (TcpConnection.class) {
                asyncConnection = asyncConnections.get(remoteHostAndPort);
                if (asyncConnection == null) {
                    final AtomicReference<TcpConnection> connRef = new AtomicReference<>();
                    CountDownLatch latch = new CountDownLatch(1);
                    client.connect(port, host, res -> {
                        try {
                            if (res.succeeded()) {
                                NetSocket socket = res.result();
                                TcpConnection conn = new TcpConnection(socket, false);
                                socket.handler(conn);
                                connRef.set(conn);
                            } else {
                                // TODO 是否不应该立刻移除节点
                                Gossiper.instance.removeEndpoint(resetEndpoint);
                                logger.warn("Failed to connect " + resetEndpoint, res.cause());
                                // throw DbException.convert(res.cause());
                            }
                        } finally {
                            latch.countDown();
                        }
                    });
                    try {
                        latch.await();
                        asyncConnection = connRef.get();
                        if (asyncConnection != null) {
                            String localHost = ConfigDescriptor.getLocalEndpoint().getHostAddress();
                            String localHostAndPort = localHost + ":" + port;
                            asyncConnection.initTransfer(resetEndpoint, remoteHostAndPort, localHostAndPort);
                            asyncConnections.put(remoteHostAndPort, asyncConnection);
                            connectionManagers.put(resetEndpoint, asyncConnection);
                        }
                    } catch (Exception e) {
                        throw DbException.convert(e);
                    }
                }
            }
        }
        return asyncConnection;
    }

    public void destroyConnection(NetEndpoint to) {
        TcpConnection conn = connectionManagers.get(to);
        if (conn == null)
            return;
        conn.close();
        connectionManagers.remove(to);
    }

    public NetEndpoint getConnectionEndpoint(NetEndpoint to) {
        return getConnection(to).endpoint();
    }

    public void reconnect(NetEndpoint old, NetEndpoint to) {
        getConnection(old).reset(to);
    }

    /**
     * called from gossiper when it notices a node is not responding.
     */
    public void convict(NetEndpoint ep) {
        if (logger.isDebugEnabled())
            logger.debug("Resetting pool for {}", ep);
        getConnection(ep).reset();
    }

    public void register(ILatencySubscriber subcriber) {
        subscribers.add(subcriber);
    }

    /**
     * Wait for callbacks and don't allow any more to be created (since they could require writing hints)
     */
    public void shutdown() {
        logger.info("Waiting for messaging service to quiesce");

        // the important part
        callbacks.shutdownBlocking();

        try {
            server.close();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public CallbackInfo getRegisteredCallback(int messageId) {
        return callbacks.get(messageId);
    }

    public CallbackInfo removeRegisteredCallback(int messageId) {
        return callbacks.remove(messageId);
    }

    /**
     * @return System.nanoTime() when callback was created.
     */
    public long getRegisteredCallbackAge(int messageId) {
        return callbacks.getAge(messageId);
    }

    public void setVersion(NetEndpoint endpoint, int version) {
        if (logger.isDebugEnabled())
            logger.debug("Setting version {} for {}", version, endpoint);

        versions.put(endpoint, version);
    }

    public void removeVersion(NetEndpoint endpoint) {
        if (logger.isDebugEnabled())
            logger.debug("Removing version for {}", endpoint);
        versions.remove(endpoint);
    }

    public int getVersion(NetEndpoint endpoint) {
        Integer v = versions.get(endpoint);
        if (v == null) {
            // we don't know the version. assume current. we'll know soon enough if that was incorrect.
            if (logger.isTraceEnabled())
                logger.trace("Assuming current protocol version for {}", endpoint);
            return MessagingService.CURRENT_VERSION;
        } else
            return Math.min(v, MessagingService.CURRENT_VERSION);
    }

    public boolean knowsVersion(NetEndpoint endpoint) {
        return versions.containsKey(endpoint);
    }

    // --------------以下是MessagingServiceMBean的API实现-------------

    @Override
    public int getVersion(String endpoint) throws UnknownHostException {
        return getVersion(NetEndpoint.getByName(endpoint));
    }

    @Override
    public Map<String, Integer> getResponsePendingTasks() {
        Map<String, Integer> pendingTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<NetEndpoint, TcpConnection> entry : connectionManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().getPendingMessages());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getResponseCompletedTasks() {
        Map<String, Long> completedTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<NetEndpoint, TcpConnection> entry : connectionManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().getCompletedMesssages());
        return completedTasks;
    }

    @Override
    public Map<String, Integer> getDroppedMessages() {
        Map<String, Integer> map = new HashMap<>(droppedMessages.size());
        for (Map.Entry<Verb, DroppedMessageMetrics> entry : droppedMessages.entrySet())
            map.put(entry.getKey().toString(), (int) entry.getValue().dropped.count());
        return map;
    }

    @Override
    public long getTotalTimeouts() {
        return ConnectionMetrics.totalTimeouts.count();
    }

    @Override
    public Map<String, Long> getTimeoutsPerHost() {
        Map<String, Long> result = new HashMap<>(connectionManagers.size());
        for (Map.Entry<NetEndpoint, TcpConnection> entry : connectionManagers.entrySet()) {
            String ip = entry.getKey().getHostAddress();
            long recent = entry.getValue().getTimeouts();
            result.put(ip, recent);
        }
        return result;
    }
}

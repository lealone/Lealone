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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.net.InetAddress;
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

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.lealone.aose.concurrent.ScheduledExecutors;
import org.lealone.aose.concurrent.Stage;
import org.lealone.aose.concurrent.StageManager;
import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.gms.EchoMessage;
import org.lealone.aose.gms.EchoVerbHandler;
import org.lealone.aose.gms.GossipDigestAck;
import org.lealone.aose.gms.GossipDigestAck2;
import org.lealone.aose.gms.GossipDigestAck2VerbHandler;
import org.lealone.aose.gms.GossipDigestAckVerbHandler;
import org.lealone.aose.gms.GossipDigestSyn;
import org.lealone.aose.gms.GossipDigestSynVerbHandler;
import org.lealone.aose.gms.GossipShutdownVerbHandler;
import org.lealone.aose.locator.ILatencySubscriber;
import org.lealone.aose.metrics.ConnectionMetrics;
import org.lealone.aose.metrics.DroppedMessageMetrics;
import org.lealone.aose.server.ClusterMetaData;
import org.lealone.aose.server.PullSchema;
import org.lealone.aose.server.PullSchemaAck;
import org.lealone.aose.server.PullSchemaAckVerbHandler;
import org.lealone.aose.server.PullSchemaVerbHandler;
import org.lealone.aose.util.ExpiringMap;
import org.lealone.aose.util.Pair;
import org.lealone.aose.util.Utils;
import org.lealone.common.exceptions.ConfigurationException;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.net.NetFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
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
        PULL_SCHEMA,
        PULL_SCHEMA_ACK,
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

            put(Verb.PULL_SCHEMA, Stage.REQUEST_RESPONSE);
            put(Verb.PULL_SCHEMA_ACK, Stage.REQUEST_RESPONSE);

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
            put(Verb.PULL_SCHEMA, PullSchema.serializer);
            put(Verb.PULL_SCHEMA_ACK, PullSchemaAck.serializer);
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

    private final ConcurrentMap<InetAddress, TcpConnection> connectionManagers = new NonBlockingHashMap<>();

    // total dropped message counts for server lifetime
    private final Map<Verb, DroppedMessageMetrics> droppedMessages = new EnumMap<>(Verb.class);
    // dropped count when last requested for the Recent api. high concurrency isn't necessary here.
    private final Map<Verb, Integer> lastDroppedInternal = new EnumMap<>(Verb.class);

    private final List<ILatencySubscriber> subscribers = new ArrayList<>();

    // protocol versions of the other nodes in the cluster
    private final ConcurrentMap<InetAddress, Integer> versions = new NonBlockingHashMap<>();

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
        registerVerbHandler(Verb.PULL_SCHEMA, new PullSchemaVerbHandler());
        registerVerbHandler(Verb.PULL_SCHEMA_ACK, new PullSchemaAckVerbHandler());
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
     * @param cb      the callback associated with this message -- this lets us know if it's a message type we're interested in
     * @param address the host that replied to the message
     * @param latency
     */
    public void maybeAddLatency(IAsyncCallback cb, InetAddress address, long latency) {
        if (cb.isLatencyForSnitch())
            addLatency(address, latency);
    }

    public void addLatency(InetAddress address, long latency) {
        for (ILatencySubscriber subscriber : subscribers)
            subscriber.receiveTiming(address, latency);
    }

    private final List<Server> servers = Lists.newArrayList();

    /**
     * Listen on the specified port.
     *
     * @param localEp InetAddress whose port to listen on.
     */
    public void start(InetAddress localEp, Map<String, String> config) throws ConfigurationException {
        initVertx(config);

        callbacks.reset(); // hack to allow tests to stop/restart MS
        for (NetServer netServer : getNetServers(localEp)) {
            Server server = new Server(netServer);
            server.start();
            servers.add(server);
        }
    }

    private List<NetServer> getNetServers(InetAddress localEp) throws ConfigurationException {
        String host = localEp.getHostAddress();
        final List<NetServer> servers = new ArrayList<>(2);
        ServerEncryptionOptions options = ConfigDescriptor.getServerEncryptionOptions();
        if (options.internode_encryption != ServerEncryptionOptions.InternodeEncryption.none) {
            NetServerOptions nso = NetFactory.getNetServerOptions(options);
            nso.setHost(host);
            nso.setPort(ConfigDescriptor.getSSLStoragePort());
            NetServer server = vertx.createNetServer(nso);
            servers.add(server);
            logger.info("Starting Encrypted Messaging Service on SSL port {}", ConfigDescriptor.getSSLStoragePort());
        }

        if (options.internode_encryption != ServerEncryptionOptions.InternodeEncryption.all) {
            NetServerOptions nso = NetFactory.getNetServerOptions(null);
            nso.setHost(host);
            nso.setPort(ConfigDescriptor.getStoragePort());
            nso.setReuseAddress(true);
            NetServer server = vertx.createNetServer(nso);
            servers.add(server);
            logger.info("Starting Messaging Service on port {}", ConfigDescriptor.getStoragePort());
        }
        return servers;
    }

    private static Vertx vertx;
    private static NetClient client;

    private synchronized static void initVertx(Map<String, String> config) {
        Integer blockedThreadCheckInterval = Integer.MAX_VALUE;
        if (config.containsKey("blocked_thread_check_interval")) {
            blockedThreadCheckInterval = Integer.parseInt(config.get("blocked_thread_check_interval"));
            if (blockedThreadCheckInterval <= 0)
                blockedThreadCheckInterval = Integer.MAX_VALUE;
        }
        if (vertx == null) {
            VertxOptions opt = new VertxOptions();
            opt.setBlockedThreadCheckInterval(blockedThreadCheckInterval);
            vertx = Vertx.vertx(opt);

            NetClientOptions options = NetFactory.getNetClientOptions(ConfigDescriptor.getClientEncryptionOptions());
            options.setConnectTimeout(10000);
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
                    String address = ConfigDescriptor.getListenAddress().getHostAddress();
                    if (e instanceof BindException) {
                        if (e.getMessage().contains("in use"))
                            throw new ConfigurationException(address + " is in use by another process.  "
                                    + "Change listen_address:storage_port in lealone.yaml "
                                    + "to values that do not conflict with other services");
                        else if (e.getMessage().contains("Cannot assign requested address"))
                            throw new ConfigurationException("Unable to bind to address " + address
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

    public int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb) {
        return sendRR(message, to, cb, message.getTimeout(), false);
    }

    public int sendRRWithFailure(MessageOut message, InetAddress to, IAsyncCallbackWithFailure cb) {
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
    public int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb, long timeout, boolean failureCallback) {
        int id = addCallback(message, to, cb, timeout, failureCallback);
        sendOneWay(failureCallback ? message.withParameter(FAILURE_CALLBACK_PARAM, ONE_BYTE) : message, id, to);
        return id;
    }

    private int addCallback(MessageOut message, InetAddress to, IAsyncCallback cb, long timeout,
            boolean failureCallback) {
        int messageId = nextId();
        CallbackInfo previous = callbacks.put(messageId,
                new CallbackInfo(to, cb, callbackDeserializers.get(message.verb), failureCallback), timeout);
        assert previous == null : String.format("Callback already exists for id %d! (%s)", messageId, previous);
        return messageId;
    }

    public void sendOneWay(MessageOut message, InetAddress to) {
        sendOneWay(message, nextId(), to);
    }

    public void sendReply(MessageOut message, int id, InetAddress to) {
        sendOneWay(message, id, to);
    }

    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     *
     * @param message messages to be sent.
     * @param to      endpoint to which the message needs to be sent
     */
    public void sendOneWay(MessageOut message, int id, InetAddress to) {
        if (logger.isTraceEnabled()) {
            if (to.equals(Utils.getBroadcastAddress()))
                logger.trace("Message-to-self {} going over MessagingService", message);
            else
                logger.trace("{} sending {} to {}@{}", Utils.getBroadcastAddress(), message.verb, id, to);
        }

        TcpConnection conn = getConnection(to);
        if (conn != null)
            conn.enqueue(message, id);
    }

    public TcpConnection getConnection(InetAddress remoteEndpoint) {
        InetAddress resetEndpoint = ClusterMetaData.getPreferredIP(remoteEndpoint);
        // 不能用resetEndpoint.getHostName()，很慢
        String hostAndPort = resetEndpoint.getHostAddress() + ":" + ConfigDescriptor.getStoragePort();

        TcpConnection asyncConnection = asyncConnections.get(hostAndPort);
        if (asyncConnection == null) {
            synchronized (TcpConnection.class) {
                asyncConnection = asyncConnections.get(hostAndPort);
                if (asyncConnection == null) {
                    CountDownLatch latch = new CountDownLatch(1);
                    client.connect(ConfigDescriptor.getStoragePort(), resetEndpoint.getHostAddress(), res -> {
                        try {
                            if (res.succeeded()) {
                                NetSocket socket = res.result();
                                TcpConnection conn = new TcpConnection(socket, false);
                                socket.handler(conn);
                                asyncConnections.put(hostAndPort, conn);
                                connectionManagers.put(resetEndpoint, conn);
                            } else {
                                throw DbException.convert(res.cause());
                            }
                        } finally {
                            latch.countDown();
                        }
                    });
                    try {
                        latch.await();
                        asyncConnection = asyncConnections.get(hostAndPort);
                        asyncConnection.initTransfer(resetEndpoint, hostAndPort);
                    } catch (Exception e) {
                        throw DbException.convert(e);
                    }
                }
            }
        }
        return asyncConnection;
    }

    public void destroyConnection(InetAddress to) {
        TcpConnection conn = connectionManagers.get(to);
        if (conn == null)
            return;
        conn.close();
        connectionManagers.remove(to);
    }

    public InetAddress getConnectionEndpoint(InetAddress to) {
        return getConnection(to).endpoint();
    }

    public void reconnect(InetAddress old, InetAddress to) {
        getConnection(old).reset(to);
    }

    /**
     * called from gossiper when it notices a node is not responding.
     */
    public void convict(InetAddress ep) {
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

        // attempt to humor tests that try to stop and restart MS
        try {
            for (Server server : servers)
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

    public void setVersion(InetAddress endpoint, int version) {
        if (logger.isDebugEnabled())
            logger.debug("Setting version {} for {}", version, endpoint);

        versions.put(endpoint, version);
    }

    public void removeVersion(InetAddress endpoint) {
        if (logger.isDebugEnabled())
            logger.debug("Removing version for {}", endpoint);
        versions.remove(endpoint);
    }

    public int getVersion(InetAddress endpoint) {
        Integer v = versions.get(endpoint);
        if (v == null) {
            // we don't know the version. assume current. we'll know soon enough if that was incorrect.
            if (logger.isTraceEnabled())
                logger.trace("Assuming current protocol version for {}", endpoint);
            return MessagingService.CURRENT_VERSION;
        } else
            return Math.min(v, MessagingService.CURRENT_VERSION);
    }

    public boolean knowsVersion(InetAddress endpoint) {
        return versions.containsKey(endpoint);
    }

    // --------------以下是MessagingServiceMBean的API实现-------------

    @Override
    public int getVersion(String endpoint) throws UnknownHostException {
        return getVersion(InetAddress.getByName(endpoint));
    }

    @Override
    public Map<String, Integer> getResponsePendingTasks() {
        Map<String, Integer> pendingTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, TcpConnection> entry : connectionManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().getPendingMessages());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getResponseCompletedTasks() {
        Map<String, Long> completedTasks = new HashMap<>(connectionManagers.size());
        for (Map.Entry<InetAddress, TcpConnection> entry : connectionManagers.entrySet())
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
        for (Map.Entry<InetAddress, TcpConnection> entry : connectionManagers.entrySet()) {
            String ip = entry.getKey().getHostAddress();
            long recent = entry.getValue().getTimeouts();
            result.put(ip, recent);
        }
        return result;
    }
}

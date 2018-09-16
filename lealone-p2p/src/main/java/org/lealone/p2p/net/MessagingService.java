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
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.lealone.common.concurrent.ScheduledExecutors;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetEndpoint;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.WritableChannel;
import org.lealone.p2p.concurrent.Stage;
import org.lealone.p2p.concurrent.StageManager;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.gms.Gossiper;
import org.lealone.p2p.locator.ILatencySubscriber;
import org.lealone.p2p.metrics.ConnectionMetrics;
import org.lealone.p2p.metrics.DroppedMessageMetrics;
import org.lealone.p2p.server.ClusterMetaData;
import org.lealone.p2p.server.P2pServer;
import org.lealone.p2p.util.ExpiringMap;
import org.lealone.p2p.util.Pair;
import org.lealone.p2p.util.Utils;

@SuppressWarnings({ "rawtypes" })
public final class MessagingService implements MessagingServiceMBean, AsyncConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);

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

    public static final EnumMap<Verb, Stage> verbStages = new EnumMap<>(Verb.class);
    public static final EnumMap<Verb, IVersionedSerializer<?>> verbSerializers = new EnumMap<>(Verb.class);

    /**
     * Verbs it's okay to drop if the request has been queued longer than the request timeout.
     * These all correspond to client requests or something triggered by them; 
     * we don't want to drop internal messages like bootstrap.
     */
    public static final EnumSet<Verb> DROPPABLE_VERBS = EnumSet.of(Verb.REQUEST_RESPONSE);

    private static final int LOG_DROPPED_INTERVAL_IN_MS = 5000;

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
    }

    /* This records all the results mapped by message Id */
    private final ExpiringMap<Integer, CallbackInfo> callbacks;

    /* Lookup table for registering message handlers based on the verb. */
    private final Map<Verb, IVerbHandler> verbHandlers = new EnumMap<>(Verb.class);

    // total dropped message counts for server lifetime
    private final Map<Verb, DroppedMessageMetrics> droppedMessages = new EnumMap<>(Verb.class);
    // dropped count when last requested for the Recent api. high concurrency isn't necessary here.
    private final Map<Verb, Integer> lastDroppedInternal = new EnumMap<>(Verb.class);

    private final List<ILatencySubscriber> subscribers = new ArrayList<>();

    // protocol versions of the other nodes in the cluster
    private final ConcurrentMap<NetEndpoint, Integer> versions = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, P2pConnection> connections = new ConcurrentHashMap<>();

    private static class MSHandle {
        public static final MessagingService instance = new MessagingService();
    }

    public static MessagingService instance() {
        return MSHandle.instance;
    }

    private MessagingService() {
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

    public void start() {
        callbacks.reset(); // hack to allow tests to stop/restart MS

        for (Verb verb : Verb.values()) {
            if (verb.stage != null)
                verbStages.put(verb, verb.stage);
            if (verb.serializer != null)
                verbSerializers.put(verb, verb.serializer);
            if (verb.verbHandler != null)
                registerVerbHandler(verb, verb.verbHandler);
        }
    }

    /**
     * Wait for callbacks and don't allow any more to be created (since they could require writing hints)
     */
    public void shutdown() {
        logger.info("Waiting for messaging service to quiesce");
        // the important part
        callbacks.shutdownBlocking();
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

        P2pConnection conn = getConnection(to);
        if (conn != null)
            conn.enqueue(message, id);
    }

    public P2pConnection getConnection(NetEndpoint remoteEndpoint) {
        remoteEndpoint = ClusterMetaData.getPreferredIP(remoteEndpoint);
        final String remoteHostAndPort = remoteEndpoint.getHostAndPort();
        P2pConnection conn = connections.get(remoteHostAndPort);
        if (conn == null) {
            synchronized (connections) {
                conn = connections.get(remoteHostAndPort);
                if (conn == null) {
                    Properties prop = new Properties();
                    prop.putAll(P2pServer.instance.getConfig());
                    NetFactory factory = NetFactoryManager.getFactory(P2pServer.instance.getConfig());
                    try {
                        conn = (P2pConnection) factory.getNetClient().createConnection(prop, remoteEndpoint, this);
                        String localHost = ConfigDescriptor.getLocalEndpoint().getHostAddress();
                        String localHostAndPort = localHost + ":" + remoteEndpoint.getPort();
                        conn.initTransfer(remoteEndpoint, remoteHostAndPort, localHostAndPort);
                        connections.put(remoteHostAndPort, conn);
                    } catch (Exception e) {
                        // TODO 是否不应该立刻移除节点
                        Gossiper.instance.removeEndpoint(remoteEndpoint);
                        logger.error("Failed to connect " + remoteEndpoint, e);
                        throw DbException.convert(e);
                    }
                }
            }
        }
        return conn;
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

    public void addConnection(P2pConnection conn) {
        P2pConnection oldConn = connections.put(conn.getHostId(), conn);
        if (oldConn != null) {
            oldConn.close();
        }
    }

    private void removeConnection(String hostId) {
        P2pConnection oldConn = connections.remove(hostId);
        if (oldConn != null) {
            oldConn.close();
        }
    }

    public void removeConnection(NetEndpoint ep) {
        removeConnection(ep.getHostAndPort());
    }

    public void removeConnection(P2pConnection conn) {
        removeConnection(conn.getHostId());
    }

    @Override
    public void removeConnection(AsyncConnection conn) {
        removeConnection((P2pConnection) conn);
    }

    @Override
    public P2pConnection createConnection(WritableChannel writableChannel, boolean isServer) {
        P2pConnection conn = new P2pConnection(writableChannel, isServer);
        conn.setHostAndPort(writableChannel.getHost() + ":" + writableChannel.getPort());
        addConnection(conn);
        return conn;
    }

    // --------------以下是MessagingServiceMBean的API实现-------------

    @Override
    public int getVersion(String endpoint) throws UnknownHostException {
        return getVersion(NetEndpoint.getByName(endpoint));
    }

    @Override
    public Map<String, Integer> getResponsePendingTasks() {
        Map<String, Integer> pendingTasks = new HashMap<>(connections.size());
        for (P2pConnection conn : connections.values())
            pendingTasks.put(conn.endpoint().getHostAddress(), conn.getPendingMessages());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getResponseCompletedTasks() {
        Map<String, Long> completedTasks = new HashMap<>(connections.size());
        for (P2pConnection conn : connections.values())
            completedTasks.put(conn.endpoint().getHostAddress(), conn.getCompletedMesssages());
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
        Map<String, Long> result = new HashMap<>(connections.size());
        for (P2pConnection conn : connections.values()) {
            result.put(conn.endpoint().getHostAddress(), conn.getTimeouts());
        }
        return result;
    }
}

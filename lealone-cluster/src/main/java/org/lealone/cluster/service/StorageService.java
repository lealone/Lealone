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
package org.lealone.cluster.service;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import org.apache.commons.lang3.StringUtils;
import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.db.ClusterMetaData;
import org.lealone.cluster.db.Keyspace;
import org.lealone.cluster.dht.BootStrapper;
import org.lealone.cluster.dht.IPartitioner;
import org.lealone.cluster.dht.Range;
import org.lealone.cluster.dht.RingPosition;
import org.lealone.cluster.dht.Token;
import org.lealone.cluster.exceptions.ConfigurationException;
import org.lealone.cluster.gms.ApplicationState;
import org.lealone.cluster.gms.EchoVerbHandler;
import org.lealone.cluster.gms.EndpointState;
import org.lealone.cluster.gms.FailureDetector;
import org.lealone.cluster.gms.GossipDigestAck2VerbHandler;
import org.lealone.cluster.gms.GossipDigestAckVerbHandler;
import org.lealone.cluster.gms.GossipDigestSynVerbHandler;
import org.lealone.cluster.gms.GossipShutdownVerbHandler;
import org.lealone.cluster.gms.Gossiper;
import org.lealone.cluster.gms.IEndpointStateChangeSubscriber;
import org.lealone.cluster.gms.TokenSerializer;
import org.lealone.cluster.gms.VersionedValue;
import org.lealone.cluster.locator.AbstractReplicationStrategy;
import org.lealone.cluster.locator.IEndpointSnitch;
import org.lealone.cluster.locator.TokenMetadata;
import org.lealone.cluster.net.MessagingService;
import org.lealone.cluster.net.ResponseVerbHandler;
import org.lealone.cluster.utils.BackgroundActivityMonitor;
import org.lealone.cluster.utils.FileUtils;
import org.lealone.cluster.utils.Pair;
import org.lealone.cluster.utils.Utils;
import org.lealone.cluster.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
public class StorageService extends NotificationBroadcasterSupport implements IEndpointStateChangeSubscriber,
        StorageServiceMBean {
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);
    private static final BackgroundActivityMonitor bgMonitor = new BackgroundActivityMonitor();

    public static final StorageService instance = new StorageService();
    public static final int RING_DELAY = getRingDelay(); // delay after which we assume ring has stablized

    private static int getRingDelay() {
        String newdelay = System.getProperty("lealone.ring_delay_ms");
        if (newdelay != null) {
            logger.info("Overriding RING_DELAY to {}ms", newdelay);
            return Integer.parseInt(newdelay);
        } else
            return 30 * 1000;
    }

    public static IPartitioner getPartitioner() {
        return DatabaseDescriptor.getPartitioner();
    }

    private static enum Mode {
        STARTING,
        NORMAL,
        JOINING,
        LEAVING,
        DECOMMISSIONED,
        MOVING,
        DRAINING,
        DRAINED
    }

    private Mode operationMode = Mode.STARTING;

    public final VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(
            getPartitioner());
    /* This abstraction maintains the token/endpoint metadata information */
    private final TokenMetadata tokenMetadata = new TokenMetadata();

    /* JMX notification serial number counter */
    private final AtomicLong notificationSerialNumber = new AtomicLong();
    private final ObjectName jmxObjectName;
    private final List<IEndpointLifecycleSubscriber> lifecycleSubscribers = new CopyOnWriteArrayList<>();

    private final Set<InetAddress> replicatingNodes = Collections.synchronizedSet(new HashSet<InetAddress>());
    private InetAddress removingNode;
    /* we bootstrap but do NOT join the ring unless told to do so */
    private boolean isSurveyMode = Boolean.parseBoolean(System.getProperty("lealone.write_survey", "false"));
    private boolean initialized;
    private volatile boolean joined = false;
    /* Are we starting this node in bootstrap mode? */
    private boolean isBootstrapMode;
    private Collection<Token> bootstrapTokens;

    private Thread drainOnShutdown;

    public StorageService() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxObjectName = new ObjectName(Utils.getJmxObjectName("StorageService"));
            mbs.registerMBean(this, jmxObjectName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.REQUEST_RESPONSE, //
                new ResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.INTERNAL_RESPONSE, //
                new ResponseVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_SHUTDOWN, //
                new GossipShutdownVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_SYN, //
                new GossipDigestSynVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_ACK, //
                new GossipDigestAckVerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.GOSSIP_DIGEST_ACK2, //
                new GossipDigestAck2VerbHandler());
        MessagingService.instance().registerVerbHandlers(MessagingService.Verb.ECHO, //
                new EchoVerbHandler());
    }

    public synchronized void start() throws ConfigurationException {
        start(RING_DELAY);
    }

    public synchronized void start(int delay) throws ConfigurationException {
        initialized = true;

        if (Boolean.parseBoolean(System.getProperty("lealone.load_ring_state", "true"))) {
            logger.info("Loading persisted ring state");
            Multimap<InetAddress, Token> loadedTokens = ClusterMetaData.loadTokens();
            Map<InetAddress, UUID> loadedHostIds = ClusterMetaData.loadHostIds();
            for (InetAddress ep : loadedTokens.keySet()) {
                if (ep.equals(Utils.getBroadcastAddress())) {
                    // entry has been mistakenly added, delete it
                    ClusterMetaData.removeEndpoint(ep);
                } else {
                    tokenMetadata.updateNormalTokens(loadedTokens.get(ep), ep);
                    if (loadedHostIds.containsKey(ep))
                        tokenMetadata.updateHostId(loadedHostIds.get(ep), ep);
                    Gossiper.instance.addSavedEndpoint(ep);
                }
            }
        }

        addShutdownHook();
        prepareToJoin();

        if (Boolean.parseBoolean(System.getProperty("lealone.join_ring", "true"))) {
            joinTokenRing(delay);
        } else {
            Collection<Token> tokens = ClusterMetaData.getSavedTokens();
            if (!tokens.isEmpty()) {
                tokenMetadata.updateNormalTokens(tokens, Utils.getBroadcastAddress());
                // order is important here, the gossiper can fire in between adding these two states.  
                // It's ok to send TOKENS without STATUS, but *not* vice versa.
                List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<>();
                states.add(Pair.create(ApplicationState.TOKENS, valueFactory.tokens(tokens)));
                states.add(Pair.create(ApplicationState.STATUS, valueFactory.hibernate(true)));
                Gossiper.instance.addLocalApplicationStates(states);
            }
            logger.info("Not joining ring as requested. Use JMX (StorageService->joinRing()) to initiate ring joining");
        }
    }

    private void addShutdownHook() {
        // daemon threads, like our executors', continue to run while shutdown hooks are invoked
        drainOnShutdown = new Thread(new WrappedRunnable() {
            @Override
            public void runMayThrow() throws InterruptedException {
                Gossiper.instance.stop();

                // In-progress writes originating here could generate hints to be written, so shut down MessagingService
                // before mutation stage, so we can get all the hints saved before shutting down
                MessagingService.instance().shutdown();
            }
        }, "StorageServiceShutdownHook");
        Runtime.getRuntime().addShutdownHook(drainOnShutdown);
    }

    private void prepareToJoin() throws ConfigurationException {
        if (!joined) {
            Map<ApplicationState, VersionedValue> appStates = new HashMap<>();

            if (DatabaseDescriptor.isReplacing()
                    && !(Boolean.parseBoolean(System.getProperty("lealone.join_ring", "true"))))
                throw new ConfigurationException("Cannot set both join_ring=false and attempt to replace a node");
            if (DatabaseDescriptor.getReplaceTokens().size() > 0 || DatabaseDescriptor.getReplaceNode() != null)
                throw new RuntimeException("Replace method removed; use lealone.replace_address instead");
            if (DatabaseDescriptor.isReplacing()) {
                if (ClusterMetaData.bootstrapComplete())
                    throw new RuntimeException("Cannot replace address with a node that is already bootstrapped");
                if (!DatabaseDescriptor.isAutoBootstrap())
                    throw new RuntimeException(
                            "Trying to replace_address with auto_bootstrap disabled will not work, check your configuration");
                bootstrapTokens = prepareReplacementInfo();
                appStates.put(ApplicationState.TOKENS, valueFactory.tokens(bootstrapTokens));
                appStates.put(ApplicationState.STATUS, valueFactory.hibernate(true));
            } else if (shouldBootstrap()) {
                checkForEndpointCollision();
            }

            // have to start the gossip service before we can see any info on other nodes.  
            // this is necessary for bootstrap to get the load info it needs.
            // (we won't be part of the storage ring though until we add a counterId to our state, below.)
            // Seed the host ID-to-endpoint map with our own ID.
            UUID localHostId = ClusterMetaData.getLocalHostId();
            getTokenMetadata().updateHostId(localHostId, Utils.getBroadcastAddress());
            appStates.put(ApplicationState.NET_VERSION, valueFactory.networkVersion());
            appStates.put(ApplicationState.HOST_ID, valueFactory.hostId(localHostId));
            appStates.put(ApplicationState.RPC_ADDRESS,
                    valueFactory.rpcaddress(DatabaseDescriptor.getBroadcastRpcAddress()));
            appStates.put(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());
            logger.info("Starting up server gossip");
            Gossiper.instance.register(this);
            Gossiper.instance.start(ClusterMetaData.incrementAndGetGeneration(), appStates);
            // gossip snitch infos (local DC and rack)
            gossipSnitchInfo();

            if (!MessagingService.instance().isListening())
                MessagingService.instance().listen(Utils.getLocalAddress());
            LoadBroadcaster.instance.startBroadcasting();
        }
    }

    private Collection<Token> prepareReplacementInfo() throws ConfigurationException {
        logger.info("Gathering node replacement information for {}", DatabaseDescriptor.getReplaceAddress());
        if (!MessagingService.instance().isListening())
            MessagingService.instance().listen(Utils.getLocalAddress());

        // make magic happen
        Gossiper.instance.doShadowRound();

        UUID hostId = null;
        // now that we've gossiped at least once, we should be able to find the node we're replacing
        if (Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress()) == null)
            throw new RuntimeException("Cannot replace_address " + DatabaseDescriptor.getReplaceAddress()
                    + " because it doesn't exist in gossip");
        hostId = Gossiper.instance.getHostId(DatabaseDescriptor.getReplaceAddress());
        try {
            if (Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress())
                    .getApplicationState(ApplicationState.TOKENS) == null)
                throw new RuntimeException("Could not find tokens for " + DatabaseDescriptor.getReplaceAddress()
                        + " to replace");
            Collection<Token> tokens = TokenSerializer.deserialize(
                    getPartitioner(),
                    new DataInputStream(new ByteArrayInputStream(getApplicationStateValue(
                            DatabaseDescriptor.getReplaceAddress(), ApplicationState.TOKENS))));

            ClusterMetaData.setLocalHostId(hostId); // use the replacee's host Id as our own so we receive hints, etc
            Gossiper.instance.resetEndpointStateMap(); // clean up since we have what we need
            return tokens;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean shouldBootstrap() {
        return DatabaseDescriptor.isAutoBootstrap() && !ClusterMetaData.bootstrapComplete()
                && !DatabaseDescriptor.getSeeds().contains(Utils.getBroadcastAddress());
    }

    private void checkForEndpointCollision() throws ConfigurationException {
        if (logger.isDebugEnabled())
            logger.debug("Starting shadow gossip round to check for endpoint collision");
        if (!MessagingService.instance().isListening())
            MessagingService.instance().listen(Utils.getLocalAddress());
        Gossiper.instance.doShadowRound();
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(Utils.getBroadcastAddress());
        if (epState != null && !Gossiper.instance.isDeadState(epState)
                && !Gossiper.instance.isGossipOnlyMember(Utils.getBroadcastAddress())) {
            throw new RuntimeException(String.format("A node with address %s already exists, cancelling join. "
                    + "Use lealone.replace_address if you want to replace this node.", Utils.getBroadcastAddress()));
        }
        Gossiper.instance.resetEndpointStateMap();
    }

    public void gossipSnitchInfo() {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        String dc = snitch.getDatacenter(Utils.getBroadcastAddress());
        String rack = snitch.getRack(Utils.getBroadcastAddress());
        Gossiper.instance.addLocalApplicationState(ApplicationState.DC,
                StorageService.instance.valueFactory.datacenter(dc));
        Gossiper.instance.addLocalApplicationState(ApplicationState.RACK,
                StorageService.instance.valueFactory.rack(rack));
    }

    private void joinTokenRing(int delay) throws ConfigurationException {
        joined = true;

        // We bootstrap if we haven't successfully bootstrapped before, as long as we are not a seed.
        // If we are a seed, or if the user manually sets auto_bootstrap to false,
        // we'll skip streaming data from other nodes and jump directly into the ring.
        //
        // The seed check allows us to skip the RING_DELAY sleep for the single-node cluster case,
        // which is useful for both new users and testing.
        //
        // We attempted to replace this with a schema-presence check, but you need a meaningful sleep
        // to get schema info from gossip which defeats the purpose.  See lealone-4427 for the gory details.
        Set<InetAddress> current = new HashSet<>();
        logger.debug("Bootstrap variables: {} {} {} {}", DatabaseDescriptor.isAutoBootstrap(), ClusterMetaData
                .bootstrapInProgress(), ClusterMetaData.bootstrapComplete(),
                DatabaseDescriptor.getSeeds().contains(Utils.getBroadcastAddress()));
        if (DatabaseDescriptor.isAutoBootstrap() && !ClusterMetaData.bootstrapComplete()
                && DatabaseDescriptor.getSeeds().contains(Utils.getBroadcastAddress()))
            logger.info("This node will not auto bootstrap because it is configured to be a seed node.");
        if (shouldBootstrap()) {
            if (ClusterMetaData.bootstrapInProgress())
                logger.warn("Detected previous bootstrap failure; retrying");
            else
                ClusterMetaData.setBootstrapState(ClusterMetaData.BootstrapState.IN_PROGRESS);
            setMode(Mode.JOINING, "waiting for ring information", true);
            // first sleep the delay to make sure we see all our peers
            //            for (int i = 0; i < delay; i += 1000) {
            //                // if we see schema, we can proceed to the next check directly
            //                if (!Schema.instance.getVersion().equals(Schema.emptyVersion)) {
            //                    logger.debug("got schema: {}", Schema.instance.getVersion());
            //                    break;
            //                }
            //                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            //            }
            setMode(Mode.JOINING, "schema complete, ready to bootstrap", true);
            setMode(Mode.JOINING, "waiting for pending range calculation", true);
            PendingRangeCalculatorService.instance.blockUntilFinished();
            setMode(Mode.JOINING, "calculation complete, ready to bootstrap", true);

            if (logger.isDebugEnabled())
                logger.debug("... got ring + schema info");

            if (Boolean.parseBoolean(System.getProperty("lealone.consistent.rangemovement", "true"))
                    && (tokenMetadata.getBootstrapTokens().valueSet().size() > 0
                            || tokenMetadata.getLeavingEndpoints().size() > 0 || tokenMetadata.getMovingEndpoints()
                            .size() > 0))
                throw new UnsupportedOperationException("Other bootstrapping/leaving/moving nodes detected, "
                        + "cannot bootstrap while lealone.consistent.rangemovement is true");

            if (!DatabaseDescriptor.isReplacing()) {
                if (tokenMetadata.isMember(Utils.getBroadcastAddress())) {
                    String s = "This node is already a member of the token ring; "
                            + "bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)";
                    throw new UnsupportedOperationException(s);
                }
                setMode(Mode.JOINING, "getting bootstrap token", true);
                bootstrapTokens = BootStrapper.getBootstrapTokens(tokenMetadata);
            } else {
                if (!DatabaseDescriptor.getReplaceAddress().equals(Utils.getBroadcastAddress())) {
                    try {
                        // Sleep additionally to make sure that the server actually is not alive
                        // and giving it more time to gossip if alive.
                        Thread.sleep(LoadBroadcaster.BROADCAST_INTERVAL);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }

                    // check for operator errors...
                    for (Token token : bootstrapTokens) {
                        InetAddress existing = tokenMetadata.getEndpoint(token);
                        if (existing != null) {
                            long nanoDelay = delay * 1000000L;
                            if (Gossiper.instance.getEndpointStateForEndpoint(existing).getUpdateTimestamp() > (System
                                    .nanoTime() - nanoDelay))
                                throw new UnsupportedOperationException("Cannot replace a live node... ");
                            current.add(existing);
                        } else {
                            throw new UnsupportedOperationException("Cannot replace token " + token
                                    + " which does not exist!");
                        }
                    }
                } else {
                    try {
                        Thread.sleep(RING_DELAY);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }

                }
                setMode(Mode.JOINING, "Replacing a node with token(s): " + bootstrapTokens, true);
            }

            bootstrap(bootstrapTokens);
            assert !isBootstrapMode; // bootstrap will block until finished
        } else {
            bootstrapTokens = ClusterMetaData.getSavedTokens();
            if (bootstrapTokens.isEmpty()) {
                bootstrapTokens = BootStrapper.getRandomTokens(tokenMetadata, DatabaseDescriptor.getNumTokens());
                if (DatabaseDescriptor.getNumTokens() == 1)
                    logger.warn("Generated random token {}. Random tokens will result in an unbalanced ring; "
                            + "see http://wiki.apache.org/lealone/Operations", bootstrapTokens);
                else
                    logger.info("Generated random tokens. tokens are {}", bootstrapTokens);
            } else {
                if (bootstrapTokens.size() != DatabaseDescriptor.getNumTokens())
                    throw new ConfigurationException("Cannot change the number of tokens from "
                            + bootstrapTokens.size() + " to " + DatabaseDescriptor.getNumTokens());
                else
                    logger.info("Using saved tokens {}", bootstrapTokens);
            }
        }

        if (!isSurveyMode) {
            // start participating in the ring.
            ClusterMetaData.setBootstrapState(ClusterMetaData.BootstrapState.COMPLETED);
            setTokens(bootstrapTokens);
            // remove the existing info about the replaced node.
            if (!current.isEmpty())
                for (InetAddress existing : current)
                    Gossiper.instance.replacedEndpoint(existing);
            assert tokenMetadata.sortedTokens().size() > 0;
        } else {
            logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. "
                    + "Use JMX (StorageService->joinRing()) to finalize ring joining.");
        }
    }

    private void bootstrap(Collection<Token> tokens) {
        isBootstrapMode = true;
        // DON'T use setToken, that makes us part of the ring locally which is incorrect until we are done bootstrapping
        ClusterMetaData.updateTokens(tokens);
        if (!DatabaseDescriptor.isReplacing()) {
            // if not an existing token then bootstrap
            List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<Pair<ApplicationState, VersionedValue>>();
            states.add(Pair.create(ApplicationState.TOKENS, valueFactory.tokens(tokens)));
            states.add(Pair.create(ApplicationState.STATUS, valueFactory.bootstrapping(tokens)));
            Gossiper.instance.addLocalApplicationStates(states);
            setMode(Mode.JOINING, "sleeping " + RING_DELAY + " ms for pending range setup", true);
            Uninterruptibles.sleepUninterruptibly(RING_DELAY, TimeUnit.MILLISECONDS);
        } else {
            // Dont set any state for the node which is bootstrapping the existing token...
            tokenMetadata.updateNormalTokens(tokens, Utils.getBroadcastAddress());
            ClusterMetaData.removeEndpoint(DatabaseDescriptor.getReplaceAddress());
        }
        if (!Gossiper.instance.seenAnySeed())
            throw new IllegalStateException("Unable to contact any seeds!");
        setMode(Mode.JOINING, "Starting to bootstrap...", true);
        new BootStrapper(Utils.getBroadcastAddress(), tokens, tokenMetadata).bootstrap(); // handles token update
        logger.info("Bootstrap completed! for the tokens {}", tokens);
    }

    public void finishBootstrapping() {
        isBootstrapMode = false;
    }

    /** This method updates the local token on disk  */
    public void setTokens(Collection<Token> tokens) {
        if (logger.isDebugEnabled())
            logger.debug("Setting tokens to {}", tokens);
        ClusterMetaData.updateTokens(tokens);
        tokenMetadata.updateNormalTokens(tokens, Utils.getBroadcastAddress());
        Collection<Token> localTokens = getLocalTokens();
        List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<Pair<ApplicationState, VersionedValue>>();
        states.add(Pair.create(ApplicationState.TOKENS, valueFactory.tokens(localTokens)));
        states.add(Pair.create(ApplicationState.STATUS, valueFactory.normal(localTokens)));
        Gossiper.instance.addLocalApplicationStates(states);
        setMode(Mode.NORMAL, false);
    }

    public Collection<Range<Token>> getLocalRanges(String keyspaceName) {
        return getRangesForEndpoint(keyspaceName, Utils.getBroadcastAddress());
    }

    public Collection<Range<Token>> getPrimaryRanges(String keyspace) {
        return getPrimaryRangesForEndpoint(keyspace, Utils.getBroadcastAddress());
    }

    public Collection<Range<Token>> getPrimaryRangesWithinDC(String keyspace) {
        return getPrimaryRangeForEndpointWithinDC(keyspace, Utils.getBroadcastAddress());
    }

    public void register(IEndpointLifecycleSubscriber subscriber) {
        lifecycleSubscribers.add(subscriber);
    }

    public void unregister(IEndpointLifecycleSubscriber subscriber) {
        lifecycleSubscribers.remove(subscriber);
    }

    // should only be called via JMX
    @Override
    public void stopGossiping() {
        if (initialized) {
            logger.warn("Stopping gossip by operator request");
            Gossiper.instance.stop();
            initialized = false;
        }
    }

    // should only be called via JMX
    @Override
    public void startGossiping() {
        if (!initialized) {
            logger.warn("Starting gossip by operator request");
            Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
            initialized = true;
        }
    }

    // should only be called via JMX
    @Override
    public boolean isGossipRunning() {
        return Gossiper.instance.isEnabled();
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * In the event of forceful termination we need to remove the shutdown hook to prevent hanging (OOM for instance)
     */
    public void removeShutdownHook() {
        if (drainOnShutdown != null)
            Runtime.getRuntime().removeShutdownHook(drainOnShutdown);
    }

    @Override
    public synchronized void joinRing() throws IOException {
        if (!joined) {
            logger.info("Joining ring by operator request");
            try {
                joinTokenRing(0);
            } catch (ConfigurationException e) {
                throw new IOException(e.getMessage());
            }
        } else if (isSurveyMode) {
            setTokens(ClusterMetaData.getSavedTokens());
            ClusterMetaData.setBootstrapState(ClusterMetaData.BootstrapState.COMPLETED);
            isSurveyMode = false;
            logger.info("Leaving write survey mode and joining ring at operator request");
            assert tokenMetadata.sortedTokens().size() > 0;
        }
    }

    @Override
    public boolean isJoined() {
        return joined;
    }

    private void setMode(Mode m, boolean log) {
        setMode(m, null, log);
    }

    private void setMode(Mode m, String msg, boolean log) {
        operationMode = m;
        String logMsg = msg == null ? m.toString() : String.format("%s: %s", m, msg);
        if (log)
            logger.info(logMsg);
        else
            logger.debug(logMsg);
    }

    public boolean isBootstrapMode() {
        return isBootstrapMode;
    }

    public TokenMetadata getTokenMetadata() {
        return tokenMetadata;
    }

    /**
     * Increment about the known Compaction severity of the events in this node
     */
    public void reportSeverity(double incr) {
        bgMonitor.incrCompactionSeverity(incr);
    }

    public void reportManualSeverity(double incr) {
        bgMonitor.incrManualSeverity(incr);
    }

    public double getSeverity(InetAddress endpoint) {
        return bgMonitor.getSeverity(endpoint);
    }

    /**
     * for a keyspace, return the ranges and corresponding listen addresses.
     * @param keyspace
     * @return the endpoint map
     */
    @Override
    public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace) {
        /* All the ranges for the tokens */
        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>, List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet()) {
            map.put(entry.getKey().asList(), stringify(entry.getValue()));
        }
        return map;
    }

    /**
     * Return the rpc address associated with an endpoint as a string.
     * @param endpoint The endpoint to get rpc address for
     * @return the rpc address
     */
    public String getRpcaddress(InetAddress endpoint) {
        if (endpoint.equals(Utils.getBroadcastAddress()))
            return DatabaseDescriptor.getBroadcastRpcAddress().getHostAddress();
        else if (Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(
                ApplicationState.RPC_ADDRESS) == null)
            return endpoint.getHostAddress();
        else
            return Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(
                    ApplicationState.RPC_ADDRESS).value;
    }

    /**
     * for a keyspace, return the ranges and corresponding RPC addresses for a given keyspace.
     * @param keyspace
     * @return the endpoint map
     */
    @Override
    public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace) {
        /* All the ranges for the tokens */
        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>, List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet()) {
            List<String> rpcaddrs = new ArrayList<>(entry.getValue().size());
            for (InetAddress endpoint : entry.getValue()) {
                rpcaddrs.add(getRpcaddress(endpoint));
            }
            map.put(entry.getKey().asList(), rpcaddrs);
        }
        return map;
    }

    @Override
    public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace) {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system keyspace.
        //        if (keyspace == null)
        //            keyspace = Schema.instance.getNonSystemKeyspaces().get(0);
        //
        //        Map<List<String>, List<String>> map = new HashMap<>();
        //        for (Map.Entry<Range<Token>, Collection<InetAddress>> entry : tokenMetadata.getPendingRanges(keyspace).entrySet()) {
        //            List<InetAddress> l = new ArrayList<>(entry.getValue());
        //            map.put(entry.getKey().asList(), stringify(l));
        //        }
        //        return map;

        return null;
    }

    public Map<Range<Token>, List<InetAddress>> getRangeToAddressMap(String keyspace) {
        return getRangeToAddressMap(keyspace, tokenMetadata.sortedTokens());
    }

    public Map<Range<Token>, List<InetAddress>> getRangeToAddressMapInLocalDC(String keyspace) {
        Predicate<InetAddress> isLocalDC = new Predicate<InetAddress>() {
            @Override
            public boolean apply(InetAddress address) {
                return isLocalDC(address);
            }
        };

        Map<Range<Token>, List<InetAddress>> origMap = getRangeToAddressMap(keyspace, getTokensInLocalDC());
        Map<Range<Token>, List<InetAddress>> filteredMap = Maps.newHashMap();
        for (Map.Entry<Range<Token>, List<InetAddress>> entry : origMap.entrySet()) {
            List<InetAddress> endpointsInLocalDC = Lists.newArrayList(Collections2.filter(entry.getValue(), isLocalDC));
            filteredMap.put(entry.getKey(), endpointsInLocalDC);
        }

        return filteredMap;
    }

    private List<Token> getTokensInLocalDC() {
        List<Token> filteredTokens = Lists.newArrayList();
        for (Token token : tokenMetadata.sortedTokens()) {
            InetAddress endpoint = tokenMetadata.getEndpoint(token);
            if (isLocalDC(endpoint))
                filteredTokens.add(token);
        }
        return filteredTokens;
    }

    private boolean isLocalDC(InetAddress targetHost) {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(Utils.getBroadcastAddress());
        return remoteDC.equals(localDC);
    }

    private Map<Range<Token>, List<InetAddress>> getRangeToAddressMap(String keyspace, List<Token> sortedTokens) {
        //        // some people just want to get a visual representation of things. Allow null and set it to the first
        //        // non-system keyspace.
        //        if (keyspace == null)
        //            keyspace = Schema.instance.getNonSystemKeyspaces().get(0);
        //
        //        List<Range<Token>> ranges = getAllRanges(sortedTokens);
        //        return constructRangeToEndpointMap(keyspace, ranges);

        return null;
    }

    @Override
    public Map<String, String> getTokenToEndpointMap() {
        Map<Token, InetAddress> mapInetAddress = tokenMetadata.getNormalAndBootstrappingTokenToEndpointMap();
        // in order to preserve tokens in ascending order, we use LinkedHashMap here
        Map<String, String> mapString = new LinkedHashMap<>(mapInetAddress.size());
        List<Token> tokens = new ArrayList<>(mapInetAddress.keySet());
        Collections.sort(tokens);
        for (Token token : tokens) {
            mapString.put(token.toString(), mapInetAddress.get(token).getHostAddress());
        }
        return mapString;
    }

    @Override
    public String getLocalHostId() {
        return getTokenMetadata().getHostId(Utils.getBroadcastAddress()).toString();
    }

    @Override
    public Map<String, String> getHostIdMap() {
        Map<String, String> mapOut = new HashMap<>();
        for (Map.Entry<InetAddress, UUID> entry : getTokenMetadata().getEndpointToHostIdMapForReading().entrySet())
            mapOut.put(entry.getKey().getHostAddress(), entry.getValue().toString());
        return mapOut;
    }

    @Override
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey,
            VersionedValue newValue) {
        // no-op
    }

    /*
     * Handle the reception of a new particular ApplicationState for a particular endpoint. Note that the value of the
     * ApplicationState has not necessarily "changed" since the last known value, if we already received the same update
     * from somewhere else.
     *
     * onChange only ever sees one ApplicationState piece change at a time (even if many ApplicationState updates were
     * received at the same time), so we perform a kind of state machine here. We are concerned with two events: knowing
     * the token associated with an endpoint, and knowing its operation mode. Nodes can start in either bootstrap or
     * normal mode, and from bootstrap mode can change mode to normal. A node in bootstrap mode needs to have
     * pendingranges set in TokenMetadata; a node in normal mode should instead be part of the token ring.
     *
     * Normal progression of ApplicationState.STATUS values for a node should be like this:
     * STATUS_BOOTSTRAPPING,token
     *   if bootstrapping. stays this way until all files are received.
     * STATUS_NORMAL,token
     *   ready to serve reads and writes.
     * STATUS_LEAVING,token
     *   get ready to leave the cluster as part of a decommission
     * STATUS_LEFT,token
     *   set after decommission is completed.
     *
     * Other STATUS values that may be seen (possibly anywhere in the normal progression):
     * STATUS_MOVING,newtoken
     *   set if node is currently moving to a new token in the ring
     * REMOVING_TOKEN,deadtoken
     *   set if the node is dead and is being removed by its REMOVAL_COORDINATOR
     * REMOVED_TOKEN,deadtoken
     *   set if the node is dead and has been removed by its REMOVAL_COORDINATOR
     *
     * Note: Any time a node state changes from STATUS_NORMAL, it will not be visible to new nodes. So it follows that
     * you should never bootstrap a new node during a removenode, decommission or move.
     */
    @Override
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
        if (state == ApplicationState.STATUS) {
            String apStateValue = value.value;
            String[] pieces = apStateValue.split(VersionedValue.DELIMITER_STR, -1);
            assert (pieces.length > 0);

            String moveName = pieces[0];

            switch (moveName) {
            case VersionedValue.STATUS_BOOTSTRAPPING:
                handleStateBootstrap(endpoint);
                break;
            case VersionedValue.STATUS_NORMAL:
                handleStateNormal(endpoint);
                break;
            case VersionedValue.REMOVING_TOKEN:
            case VersionedValue.REMOVED_TOKEN:
                handleStateRemoving(endpoint, pieces);
                break;
            case VersionedValue.STATUS_LEAVING:
                handleStateLeaving(endpoint);
                break;
            case VersionedValue.STATUS_LEFT:
                handleStateLeft(endpoint, pieces);
                break;
            case VersionedValue.STATUS_MOVING:
                handleStateMoving(endpoint, pieces);
                break;
            }
        } else {
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (epState == null || Gossiper.instance.isDeadState(epState)) {
                logger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
                return;
            }

            switch (state) {
            case RELEASE_VERSION:
                ClusterMetaData.updatePeerInfo(endpoint, "release_version", value.value);
                break;
            case DC:
                ClusterMetaData.updatePeerInfo(endpoint, "data_center", value.value);
                break;
            case RACK:
                ClusterMetaData.updatePeerInfo(endpoint, "rack", value.value);
                break;
            case RPC_ADDRESS:
                try {
                    ClusterMetaData.updatePeerInfo(endpoint, "rpc_address", InetAddress.getByName(value.value));
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                break;
            case SCHEMA:
                ClusterMetaData.updatePeerInfo(endpoint, "schema_version", UUID.fromString(value.value));
                break;
            case HOST_ID:
                ClusterMetaData.updatePeerInfo(endpoint, "host_id", UUID.fromString(value.value));
                break;
            }
        }
    }

    private void updatePeerInfo(InetAddress endpoint) {
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet()) {
            switch (entry.getKey()) {
            case RELEASE_VERSION:
                ClusterMetaData.updatePeerInfo(endpoint, "release_version", entry.getValue().value);
                break;
            case DC:
                ClusterMetaData.updatePeerInfo(endpoint, "data_center", entry.getValue().value);
                break;
            case RACK:
                ClusterMetaData.updatePeerInfo(endpoint, "rack", entry.getValue().value);
                break;
            case RPC_ADDRESS:
                try {
                    ClusterMetaData.updatePeerInfo(endpoint, "rpc_address",
                            InetAddress.getByName(entry.getValue().value));
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                break;
            case SCHEMA:
                ClusterMetaData.updatePeerInfo(endpoint, "schema_version", UUID.fromString(entry.getValue().value));
                break;
            case HOST_ID:
                ClusterMetaData.updatePeerInfo(endpoint, "host_id", UUID.fromString(entry.getValue().value));
                break;
            }
        }
    }

    private byte[] getApplicationStateValue(InetAddress endpoint, ApplicationState appstate) {
        String vvalue = Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(appstate).value;
        return vvalue.getBytes(ISO_8859_1);
    }

    private Collection<Token> getTokensFor(InetAddress endpoint) {
        try {
            return TokenSerializer.deserialize(getPartitioner(), new DataInputStream(new ByteArrayInputStream(
                    getApplicationStateValue(endpoint, ApplicationState.TOKENS))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     */
    private void handleStateBootstrap(InetAddress endpoint) {
        Collection<Token> tokens;
        // explicitly check for TOKENS, because a bootstrapping node might be bootstrapping in legacy mode; that is, 
        // not using vnodes and no token specified
        tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);

        // if this node is present in token metadata, either we have missed intermediate states
        // or the node had crashed. Print warning if needed, clear obsolete stuff and
        // continue.
        if (tokenMetadata.isMember(endpoint)) {
            // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
            // isLeaving is true, we have only missed LEFT. Waiting time between completing
            // leave operation and rebootstrapping is relatively short, so the latter is quite
            // common (not enough time for gossip to spread). Therefore we report only the
            // former in the log.
            if (!tokenMetadata.isLeaving(endpoint))
                logger.info("Node {} state jump to bootstrap", endpoint);
            tokenMetadata.removeEndpoint(endpoint);
        }

        tokenMetadata.addBootstrapTokens(tokens, endpoint);
        PendingRangeCalculatorService.instance.update();

        if (Gossiper.instance.usesHostId(endpoint))
            tokenMetadata.updateHostId(Gossiper.instance.getHostId(endpoint), endpoint);
    }

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endpoint node
     */
    private void handleStateNormal(final InetAddress endpoint) {
        Collection<Token> tokens;

        tokens = getTokensFor(endpoint);

        Set<Token> tokensToUpdateInMetadata = new HashSet<>();
        Set<Token> tokensToUpdateInSystemKeyspace = new HashSet<>();
        Set<Token> localTokensToRemove = new HashSet<>();
        Set<InetAddress> endpointsToRemove = new HashSet<>();

        if (logger.isDebugEnabled())
            logger.debug("Node {} state normal, token {}", endpoint, tokens);

        if (tokenMetadata.isMember(endpoint))
            logger.info("Node {} state jump to normal", endpoint);

        updatePeerInfo(endpoint);
        // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see lealone-4300).
        if (Gossiper.instance.usesHostId(endpoint)) {
            UUID hostId = Gossiper.instance.getHostId(endpoint);
            InetAddress existing = tokenMetadata.getEndpointForHostId(hostId);
            if (DatabaseDescriptor.isReplacing()
                    && Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress()) != null
                    && (hostId.equals(Gossiper.instance.getHostId(DatabaseDescriptor.getReplaceAddress()))))
                logger.warn("Not updating token metadata for {} because I am replacing it", endpoint);
            else {
                if (existing != null && !existing.equals(endpoint)) {
                    if (existing.equals(Utils.getBroadcastAddress())) {
                        logger.warn("Not updating host ID {} for {} because it's mine", hostId, endpoint);
                        tokenMetadata.removeEndpoint(endpoint);
                        endpointsToRemove.add(endpoint);
                    } else if (Gossiper.instance.compareEndpointStartup(endpoint, existing) > 0) {
                        logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", hostId,
                                existing, endpoint, endpoint);
                        tokenMetadata.removeEndpoint(existing);
                        endpointsToRemove.add(existing);
                        tokenMetadata.updateHostId(hostId, endpoint);
                    } else {
                        logger.warn("Host ID collision for {} between {} and {}; ignored {}", hostId, existing,
                                endpoint, endpoint);
                        tokenMetadata.removeEndpoint(endpoint);
                        endpointsToRemove.add(endpoint);
                    }
                } else
                    tokenMetadata.updateHostId(hostId, endpoint);
            }
        }

        for (final Token token : tokens) {
            // we don't want to update if this node is responsible for the token
            // and it has a later startup time than endpoint.
            InetAddress currentOwner = tokenMetadata.getEndpoint(token);
            if (currentOwner == null) {
                logger.debug("New node {} at token {}", endpoint, token);
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);
            } else if (endpoint.equals(currentOwner)) {
                // set state back to normal, since the node may have tried to leave, but failed and is now back up
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);
            } else if (Gossiper.instance.compareEndpointStartup(endpoint, currentOwner) > 0) {
                tokensToUpdateInMetadata.add(token);
                tokensToUpdateInSystemKeyspace.add(token);

                // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
                // a host no longer has any tokens, we'll want to remove it.
                Multimap<InetAddress, Token> epToTokenCopy = getTokenMetadata().getEndpointToTokenMapForReading();
                epToTokenCopy.get(currentOwner).remove(token);
                if (epToTokenCopy.get(currentOwner).size() < 1)
                    endpointsToRemove.add(currentOwner);

                logger.info(String.format("Nodes %s and %s have the same token %s.  %s is the new owner", endpoint,
                        currentOwner, token, endpoint));
            } else {
                logger.info(String.format("Nodes %s and %s have the same token %s.  Ignoring %s", endpoint,
                        currentOwner, token, endpoint));
            }
        }

        tokenMetadata.updateNormalTokens(tokensToUpdateInMetadata, endpoint);
        for (InetAddress ep : endpointsToRemove) {
            removeEndpoint(ep);
            if (DatabaseDescriptor.isReplacing() && DatabaseDescriptor.getReplaceAddress().equals(ep))
                Gossiper.instance.replacementQuarantine(ep); // quarantine locally longer than normally; see Cassandra-8260
        }
        if (!tokensToUpdateInSystemKeyspace.isEmpty())
            ClusterMetaData.updateTokens(endpoint, tokensToUpdateInSystemKeyspace);
        if (!localTokensToRemove.isEmpty())
            ClusterMetaData.updateLocalTokens(Collections.<Token> emptyList(), localTokensToRemove);

        if (tokenMetadata.isMoving(endpoint)) // if endpoint was moving to a new token
        {
            tokenMetadata.removeFromMoving(endpoint);
            for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onMove(endpoint);
        } else {
            for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onJoinCluster(endpoint);
        }

        PendingRangeCalculatorService.instance.update();
    }

    /**
     * Handle node preparing to leave the ring
     *
     * @param endpoint node
     */
    private void handleStateLeaving(InetAddress endpoint) {
        Collection<Token> tokens;
        tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state leaving, tokens {}", endpoint, tokens);

        // If the node is previously unknown or tokens do not match, update tokenmetadata to
        // have this node as 'normal' (it must have been using this token before the
        // leave). This way we'll get pending ranges right.
        if (!tokenMetadata.isMember(endpoint)) {
            logger.info("Node {} state jump to leaving", endpoint);
            tokenMetadata.updateNormalTokens(tokens, endpoint);
        } else if (!tokenMetadata.getTokens(endpoint).containsAll(tokens)) {
            logger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
            tokenMetadata.updateNormalTokens(tokens, endpoint);
        }

        // at this point the endpoint is certainly a member with this token, so let's proceed
        // normally
        tokenMetadata.addLeavingEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param endpoint If reason for leaving is decommission, endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    private void handleStateLeft(InetAddress endpoint, String[] pieces) {
        assert pieces.length >= 2;
        Collection<Token> tokens;
        tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state left, tokens {}", endpoint, tokens);

        excise(tokens, endpoint, extractExpireTime(pieces));
    }

    /**
     * Handle node moving inside the ring.
     *
     * @param endpoint moving endpoint address
     * @param pieces STATE_MOVING, token
     */
    private void handleStateMoving(InetAddress endpoint, String[] pieces) {
        assert pieces.length >= 2;
        Token token = getPartitioner().getTokenFactory().fromString(pieces[1]);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state moving, new token {}", endpoint, token);

        tokenMetadata.addMovingEndpoint(token, endpoint);

        PendingRangeCalculatorService.instance.update();
    }

    /**
     * Handle notification that a node being actively removed from the ring via 'removenode'
     *
     * @param endpoint node
     * @param pieces either REMOVED_TOKEN (node is gone) or REMOVING_TOKEN (replicas need to be restored)
     */
    private void handleStateRemoving(InetAddress endpoint, String[] pieces) {
        assert (pieces.length > 0);

        if (endpoint.equals(Utils.getBroadcastAddress())) {
            logger.info("Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");
            try {
                //drain();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return;
        }
        if (tokenMetadata.isMember(endpoint)) {
            String state = pieces[0];
            Collection<Token> removeTokens = tokenMetadata.getTokens(endpoint);

            if (VersionedValue.REMOVED_TOKEN.equals(state)) {
                excise(removeTokens, endpoint, extractExpireTime(pieces));
            } else if (VersionedValue.REMOVING_TOKEN.equals(state)) {
                if (logger.isDebugEnabled())
                    logger.debug("Tokens {} removed manually (endpoint was {})", removeTokens, endpoint);

                // Note that the endpoint is being removed
                tokenMetadata.addLeavingEndpoint(endpoint);
                PendingRangeCalculatorService.instance.update();

                // find the endpoint coordinating this removal that we need to notify when we're done
                String[] coordinator = Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(
                        ApplicationState.REMOVAL_COORDINATOR).value.split(VersionedValue.DELIMITER_STR, -1);
                UUID hostId = UUID.fromString(coordinator[1]);
                // grab any data we are now responsible for and notify responsible node
                restoreReplicaCount(endpoint, tokenMetadata.getEndpointForHostId(hostId));
            }
        } else // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
        {
            if (VersionedValue.REMOVED_TOKEN.equals(pieces[0]))
                addExpireTimeIfFound(endpoint, extractExpireTime(pieces));
            removeEndpoint(endpoint);
        }
    }

    private void excise(Collection<Token> tokens, InetAddress endpoint) {
        logger.info("Removing tokens {} for {}", tokens, endpoint);
        removeEndpoint(endpoint);
        tokenMetadata.removeEndpoint(endpoint);
        tokenMetadata.removeBootstrapTokens(tokens);

        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onLeaveCluster(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    private void excise(Collection<Token> tokens, InetAddress endpoint, long expireTime) {
        addExpireTimeIfFound(endpoint, expireTime);
        excise(tokens, endpoint);
    }

    /** unlike excise we just need this endpoint gone without going through any notifications **/
    private void removeEndpoint(InetAddress endpoint) {
        Gossiper.instance.removeEndpoint(endpoint);
        ClusterMetaData.removeEndpoint(endpoint);
    }

    protected void addExpireTimeIfFound(InetAddress endpoint, long expireTime) {
        if (expireTime != 0L) {
            Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
        }
    }

    protected long extractExpireTime(String[] pieces) {
        return Long.parseLong(pieces[2]);
    }

    /**
     * Called when an endpoint is removed from the ring. This function checks
     * whether this node becomes responsible for new ranges as a
     * consequence and streams data if needed.
     *
     * This is rather ineffective, but it does not matter so much
     * since this is called very seldom
     *
     * @param endpoint the node that left
     */
    private void restoreReplicaCount(InetAddress endpoint, final InetAddress notifyEndpoint) {
        //        Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> rangesToFetch = HashMultimap.create();
        //
        //        InetAddress myAddress = FBUtilities.getBroadcastAddress();
        //
        //        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces()) {
        //            Multimap<Range<Token>, InetAddress> changedRanges = getChangedRangesForLeaving(keyspaceName, endpoint);
        //            Set<Range<Token>> myNewRanges = new HashSet<>();
        //            for (Map.Entry<Range<Token>, InetAddress> entry : changedRanges.entries()) {
        //                if (entry.getValue().equals(myAddress))
        //                    myNewRanges.add(entry.getKey());
        //            }
        //            Multimap<InetAddress, Range<Token>> sourceRanges = getNewSourceRanges(keyspaceName, myNewRanges);
        //            for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : sourceRanges.asMap().entrySet()) {
        //                rangesToFetch.put(keyspaceName, entry);
        //            }
        //        }
        //
        //        StreamPlan stream = new StreamPlan("Restore replica count");
        //        for (String keyspaceName : rangesToFetch.keySet()) {
        //            for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : rangesToFetch.get(keyspaceName)) {
        //                InetAddress source = entry.getKey();
        //                InetAddress preferred = SystemKeyspace.getPreferredIP(source);
        //                Collection<Range<Token>> ranges = entry.getValue();
        //                if (logger.isDebugEnabled())
        //                    logger.debug("Requesting from {} ranges {}", source, StringUtils.join(ranges, ", "));
        //                stream.requestRanges(source, preferred, keyspaceName, ranges);
        //            }
        //        }
        //        StreamResultFuture future = stream.execute();
        //        Futures.addCallback(future, new FutureCallback<StreamState>() {
        //            public void onSuccess(StreamState finalState) {
        //                sendReplicationNotification(notifyEndpoint);
        //            }
        //
        //            @Override
        //            public void onFailure(Throwable t) {
        //                logger.warn("Streaming to restore replica count failed", t);
        //                // We still want to send the notification
        //                sendReplicationNotification(notifyEndpoint);
        //            }
        //        });
    }

    @Override
    public void onJoin(InetAddress endpoint, EndpointState epState) {
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet()) {
            onChange(endpoint, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void onAlive(InetAddress endpoint, EndpointState state) {
        if (tokenMetadata.isMember(endpoint)) {
            for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onUp(endpoint);
        }
    }

    @Override
    public void onRemove(InetAddress endpoint) {
        tokenMetadata.removeEndpoint(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    @Override
    public void onDead(InetAddress endpoint, EndpointState state) {
        MessagingService.instance().convict(endpoint);
        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onDown(endpoint);
    }

    @Override
    public void onRestart(InetAddress endpoint, EndpointState state) {
        // If we have restarted before the node was even marked down, we need to reset the connection pool
        if (state.isAlive())
            onDead(endpoint, state);
    }

    /** raw load value */
    public double getLoad() {
        //        double bytes = 0;
        //        for (String keyspaceName : Schema.instance.getKeyspaces()) {
        //            Keyspace keyspace = Schema.instance.getKeyspaceInstance(keyspaceName);
        //            if (keyspace == null)
        //                continue;
        //            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
        //                bytes += cfs.getLiveDiskSpaceUsed();
        //        }
        //        return bytes;

        return 0.0;
    }

    @Override
    public String getLoadString() {
        return FileUtils.stringifyFileSize(getLoad());
    }

    @Override
    public Map<String, String> getLoadMap() {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<InetAddress, Double> entry : LoadBroadcaster.instance.getLoadInfo().entrySet()) {
            map.put(entry.getKey().getHostAddress(), FileUtils.stringifyFileSize(entry.getValue()));
        }
        // gossiper doesn't see its own updates, so we need to special-case the local node
        map.put(Utils.getBroadcastAddress().getHostAddress(), getLoadString());
        return map;
    }

    public Collection<Token> getLocalTokens() {
        Collection<Token> tokens = ClusterMetaData.getSavedTokens();
        assert tokens != null && !tokens.isEmpty(); // should not be called before initServer sets this
        return tokens;
    }

    /* These methods belong to the MBean interface */

    @Override
    public List<String> getTokens() {
        return getTokens(Utils.getBroadcastAddress());
    }

    @Override
    public List<String> getTokens(String endpoint) throws UnknownHostException {
        return getTokens(InetAddress.getByName(endpoint));
    }

    private List<String> getTokens(InetAddress endpoint) {
        List<String> strTokens = new ArrayList<>();
        for (Token tok : getTokenMetadata().getTokens(endpoint))
            strTokens.add(tok.toString());
        return strTokens;
    }

    @Override
    public String getReleaseVersion() {
        return Utils.getReleaseVersionString();
    }

    @Override
    public List<String> getLeavingNodes() {
        return stringify(tokenMetadata.getLeavingEndpoints());
    }

    @Override
    public List<String> getMovingNodes() {
        List<String> endpoints = new ArrayList<>();

        for (Pair<Token, InetAddress> node : tokenMetadata.getMovingEndpoints()) {
            endpoints.add(node.right.getHostAddress());
        }

        return endpoints;
    }

    @Override
    public List<String> getJoiningNodes() {
        return stringify(tokenMetadata.getBootstrapTokens().valueSet());
    }

    @Override
    public List<String> getLiveNodes() {
        return stringify(Gossiper.instance.getLiveMembers());
    }

    @Override
    public List<String> getUnreachableNodes() {
        return stringify(Gossiper.instance.getUnreachableMembers());
    }

    private List<String> stringify(Iterable<InetAddress> endpoints) {
        List<String> stringEndpoints = new ArrayList<>();
        for (InetAddress ep : endpoints) {
            stringEndpoints.add(ep.getHostAddress());
        }
        return stringEndpoints;
    }

    @Override
    public int getCurrentGenerationNumber() {
        return Gossiper.instance.getCurrentGenerationNumber(Utils.getBroadcastAddress());
    }

    /**
     * Sends JMX notification to subscribers.
     *
     * @param type Message type
     * @param message Message itself
     * @param userObject Arbitrary object to attach to notification
     */
    public void sendNotification(String type, String message, Object userObject) {
        Notification jmxNotification = new Notification(type, jmxObjectName,
                notificationSerialNumber.incrementAndGet(), message);
        jmxNotification.setUserData(userObject);
        sendNotification(jmxNotification);
    }

    /**
     * Create collection of ranges that match ring layout from given tokens.
     *
     * @param beginToken beginning token of the range
     * @param endToken end token of the range
     * @return collection of ranges that match ring layout in TokenMetadata
     */
    @VisibleForTesting
    Collection<Range<Token>> createRepairRangeFrom(String beginToken, String endToken) {
        Token parsedBeginToken = getPartitioner().getTokenFactory().fromString(beginToken);
        Token parsedEndToken = getPartitioner().getTokenFactory().fromString(endToken);

        // Break up given range to match ring layout in TokenMetadata
        ArrayList<Range<Token>> repairingRange = new ArrayList<>();

        ArrayList<Token> tokens = new ArrayList<>(tokenMetadata.sortedTokens());
        if (!tokens.contains(parsedBeginToken)) {
            tokens.add(parsedBeginToken);
        }
        if (!tokens.contains(parsedEndToken)) {
            tokens.add(parsedEndToken);
        }
        // tokens now contain all tokens including our endpoints
        Collections.sort(tokens);

        int start = tokens.indexOf(parsedBeginToken), end = tokens.indexOf(parsedEndToken);
        for (int i = start; i != end; i = (i + 1) % tokens.size()) {
            Range<Token> range = new Range<>(tokens.get(i), tokens.get((i + 1) % tokens.size()));
            repairingRange.add(range);
        }

        return repairingRange;
    }

    /* End of MBean interface methods */

    /**
     * Get the "primary ranges" for the specified keyspace and endpoint.
     * "Primary ranges" are the ranges that the node is responsible for storing replica primarily.
     * The node that stores replica primarily is defined as the first node returned
     * by {@link AbstractReplicationStrategy#calculateNaturalEndpoints}.
     *
     * @param keyspace Keyspace name to check primary ranges
     * @param ep endpoint we are interested in.
     * @return primary ranges for the specified endpoint.
     */
    public Collection<Range<Token>> getPrimaryRangesForEndpoint(String keyspace, InetAddress ep) {
        AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
        Collection<Range<Token>> primaryRanges = new HashSet<>();
        TokenMetadata metadata = tokenMetadata.cloneOnlyTokenMap();
        for (Token token : metadata.sortedTokens()) {
            List<InetAddress> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
            if (endpoints.size() > 0 && endpoints.get(0).equals(ep))
                primaryRanges.add(new Range<>(metadata.getPredecessor(token), token));
        }
        return primaryRanges;
    }

    /**
     * Get the "primary ranges" within local DC for the specified keyspace and endpoint.
     *
     * @see #getPrimaryRangesForEndpoint(String, java.net.InetAddress)
     * @param keyspace Keyspace name to check primary ranges
     * @param referenceEndpoint endpoint we are interested in.
     * @return primary ranges within local DC for the specified endpoint.
     */
    public Collection<Range<Token>> getPrimaryRangeForEndpointWithinDC(String keyspace, InetAddress referenceEndpoint) {
        TokenMetadata metadata = tokenMetadata.cloneOnlyTokenMap();
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(referenceEndpoint);
        Collection<InetAddress> localDcNodes = metadata.getTopology().getDatacenterEndpoints().get(localDC);
        AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();

        Collection<Range<Token>> localDCPrimaryRanges = new HashSet<>();
        for (Token token : metadata.sortedTokens()) {
            List<InetAddress> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
            for (InetAddress endpoint : endpoints) {
                if (localDcNodes.contains(endpoint)) {
                    if (endpoint.equals(referenceEndpoint)) {
                        localDCPrimaryRanges.add(new Range<>(metadata.getPredecessor(token), token));
                    }
                    break;
                }
            }
        }

        return localDCPrimaryRanges;
    }

    /**
     * Get all ranges an endpoint is responsible for (by keyspace)
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    Collection<Range<Token>> getRangesForEndpoint(String keyspaceName, InetAddress ep) {
        return Keyspace.open(keyspaceName).getReplicationStrategy().getAddressRanges().get(ep);
    }

    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of
     * ranges.
     * @return ranges in sorted order
    */
    public List<Range<Token>> getAllRanges(List<Token> sortedTokens) {
        if (logger.isDebugEnabled())
            logger.debug("computing ranges for {}", StringUtils.join(sortedTokens, ", "));

        if (sortedTokens.isEmpty())
            return Collections.emptyList();
        int size = sortedTokens.size();
        List<Range<Token>> ranges = new ArrayList<>(size + 1);
        for (int i = 1; i < size; ++i) {
            Range<Token> range = new Range<>(sortedTokens.get(i - 1), sortedTokens.get(i));
            ranges.add(range);
        }
        Range<Token> range = new Range<>(sortedTokens.get(size - 1), sortedTokens.get(0));
        ranges.add(range);

        return ranges;
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param cf Column family name
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    @Override
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key) {
        //        CFMetaData cfMetaData = Schema.instance.getKSMetaData(keyspaceName).cfMetaData().get(cf);
        //        return getNaturalEndpoints(keyspaceName, getPartitioner().getToken(cfMetaData.getKeyValidator().fromString(key)));

        return null;
    }

    @Override
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key) {
        return getNaturalEndpoints(keyspaceName, getPartitioner().getToken(key));
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param pos position for which we need to find the endpoint
     * @return the endpoint responsible for this token
     */
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, RingPosition<?> pos) {
        return Keyspace.open(keyspaceName).getReplicationStrategy().getNaturalEndpoints(pos);
    }

    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspace keyspace name also known as keyspace
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    public List<InetAddress> getLiveNaturalEndpoints(Keyspace keyspace, ByteBuffer key) {
        return getLiveNaturalEndpoints(keyspace, getPartitioner().decorateKey(key));
    }

    public List<InetAddress> getLiveNaturalEndpoints(Keyspace keyspace, RingPosition<?> pos) {
        List<InetAddress> endpoints = keyspace.getReplicationStrategy().getNaturalEndpoints(pos);
        List<InetAddress> liveEps = new ArrayList<>(endpoints.size());

        for (InetAddress endpoint : endpoints) {
            if (FailureDetector.instance.isAlive(endpoint))
                liveEps.add(endpoint);
        }

        return liveEps;
    }

    /**
     * Get the status of a token removal.
     */
    @SuppressWarnings("deprecation")
    @Override
    public String getRemovalStatus() {
        if (removingNode == null) {
            return "No token removals in process.";
        }
        return String.format("Removing token (%s). Waiting for replication confirmation from [%s].",
                tokenMetadata.getToken(removingNode), StringUtils.join(replicatingNodes, ","));
    }

    /**
     * Force a remove operation to complete. This may be necessary if a remove operation
     * blocks forever due to node/stream failure. removeToken() must be called
     * first, this is a last resort measure.  No further attempt will be made to restore replicas.
     */
    @Override
    public void forceRemoveCompletion() {
        if (!replicatingNodes.isEmpty() || !tokenMetadata.getLeavingEndpoints().isEmpty()) {
            logger.warn("Removal not confirmed for for {}", StringUtils.join(this.replicatingNodes, ","));
            for (InetAddress endpoint : tokenMetadata.getLeavingEndpoints()) {
                UUID hostId = tokenMetadata.getHostId(endpoint);
                Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);
                excise(tokenMetadata.getTokens(endpoint), endpoint);
            }
            replicatingNodes.clear();
            removingNode = null;
        } else {
            logger.warn("No tokens to force removal on, call 'removenode' first");
        }
    }

    /**
     * Remove a node that has died, attempting to restore the replica count.
     * If the node is alive, decommission should be attempted.  If decommission
     * fails, then removeToken should be called.  If we fail while trying to
     * restore the replica count, finally forceRemoveCompleteion should be
     * called to forcibly remove the node without regard to replica count.
     *
     * @param hostIdString token for the node
     */
    @Override
    public void removeNode(String hostIdString) {
        //        InetAddress myAddress = FBUtilities.getBroadcastAddress();
        //        UUID localHostId = tokenMetadata.getHostId(myAddress);
        //        UUID hostId = UUID.fromString(hostIdString);
        //        InetAddress endpoint = tokenMetadata.getEndpointForHostId(hostId);
        //
        //        if (endpoint == null)
        //            throw new UnsupportedOperationException("Host ID not found.");
        //
        //        Collection<Token> tokens = tokenMetadata.getTokens(endpoint);
        //
        //        if (endpoint.equals(myAddress))
        //            throw new UnsupportedOperationException("Cannot remove self");
        //
        //        if (Gossiper.instance.getLiveMembers().contains(endpoint))
        //            throw new UnsupportedOperationException("Node " + endpoint
        //                    + " is alive and owns this ID. Use decommission command to remove it from the ring");
        //
        //        // A leaving endpoint that is dead is already being removed.
        //        if (tokenMetadata.isLeaving(endpoint))
        //            logger.warn("Node {} is already being removed, continuing removal anyway", endpoint);
        //
        //        if (!replicatingNodes.isEmpty())
        //            throw new UnsupportedOperationException(
        //                    "This node is already processing a removal. Wait for it to complete, or use 'removenode force' if this has failed.");
        //
        //        // Find the endpoints that are going to become responsible for data
        //        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces()) {
        //            // if the replication factor is 1 the data is lost so we shouldn't wait for confirmation
        //            if (Keyspace.open(keyspaceName).getReplicationStrategy().getReplicationFactor() == 1)
        //                continue;
        //
        //            // get all ranges that change ownership (that is, a node needs
        //            // to take responsibility for new range)
        //            Multimap<Range<Token>, InetAddress> changedRanges = getChangedRangesForLeaving(keyspaceName, endpoint);
        //            IFailureDetector failureDetector = FailureDetector.instance;
        //            for (InetAddress ep : changedRanges.values()) {
        //                if (failureDetector.isAlive(ep))
        //                    replicatingNodes.add(ep);
        //                else
        //                    logger.warn("Endpoint {} is down and will not receive data for re-replication of {}", ep, endpoint);
        //            }
        //        }
        //        removingNode = endpoint;
        //
        //        tokenMetadata.addLeavingEndpoint(endpoint);
        //        PendingRangeCalculatorService.instance.update();
        //
        //        // the gossiper will handle spoofing this node's state to REMOVING_TOKEN for us
        //        // we add our own token so other nodes to let us know when they're done
        //        Gossiper.instance.advertiseRemoving(endpoint, hostId, localHostId);
        //
        //        // kick off streaming commands
        //        restoreReplicaCount(endpoint, myAddress);
        //
        //        // wait for ReplicationFinishedVerbHandler to signal we're done
        //        while (!replicatingNodes.isEmpty()) {
        //            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        //        }
        //
        //        excise(tokens, endpoint);
        //
        //        // gossiper will indicate the token has left
        //        Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);
        //
        //        replicatingNodes.clear();
        //        removingNode = null;
    }

    public void confirmReplication(InetAddress node) {
        // replicatingNodes can be empty in the case where this node used to be a removal coordinator,
        // but restarted before all 'replication finished' messages arrived. In that case, we'll
        // still go ahead and acknowledge it.
        if (!replicatingNodes.isEmpty()) {
            replicatingNodes.remove(node);
        } else {
            logger.info(
                    "Received unexpected REPLICATION_FINISHED message from {}. Was this node recently a removal coordinator?",
                    node);
        }
    }

    @Override
    public String getOperationMode() {
        return operationMode.toString();
    }

    @Override
    public boolean isStarting() {
        return operationMode == Mode.STARTING;
    }

    @Override
    public Map<InetAddress, Float> getOwnership() {
        List<Token> sortedTokens = tokenMetadata.sortedTokens();
        // describeOwnership returns tokens in an unspecified order, let's re-order them
        Map<Token, Float> tokenMap = new TreeMap<Token, Float>(getPartitioner().describeOwnership(sortedTokens));
        Map<InetAddress, Float> nodeMap = new LinkedHashMap<>();
        for (Map.Entry<Token, Float> entry : tokenMap.entrySet()) {
            InetAddress endpoint = tokenMetadata.getEndpoint(entry.getKey());
            Float tokenOwnership = entry.getValue();
            if (nodeMap.containsKey(endpoint))
                nodeMap.put(endpoint, nodeMap.get(endpoint) + tokenOwnership);
            else
                nodeMap.put(endpoint, tokenOwnership);
        }
        return nodeMap;
    }

    /** Returns the name of the cluster */
    @Override
    public String getClusterName() {
        return DatabaseDescriptor.getClusterName();
    }

    /** Returns the cluster partitioner */
    @Override
    public String getPartitionerName() {
        return DatabaseDescriptor.getPartitionerName();
    }
}

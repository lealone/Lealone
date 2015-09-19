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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.lealone.cluster.config.Config;
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
import org.lealone.cluster.gms.EndpointState;
import org.lealone.cluster.gms.FailureDetector;
import org.lealone.cluster.gms.Gossiper;
import org.lealone.cluster.gms.IEndpointStateChangeSubscriber;
import org.lealone.cluster.gms.TokenSerializer;
import org.lealone.cluster.gms.VersionedValue;
import org.lealone.cluster.gms.VersionedValue.VersionedValueFactory;
import org.lealone.cluster.locator.IEndpointSnitch;
import org.lealone.cluster.locator.TokenMetaData;
import org.lealone.cluster.net.MessagingService;
import org.lealone.cluster.utils.BackgroundActivityMonitor;
import org.lealone.cluster.utils.FileUtils;
import org.lealone.cluster.utils.Pair;
import org.lealone.cluster.utils.Utils;
import org.lealone.cluster.utils.WrappedRunnable;
import org.lealone.dbobject.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
public class StorageService implements IEndpointStateChangeSubscriber {
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);
    private static final BackgroundActivityMonitor bgMonitor = new BackgroundActivityMonitor();

    public static final int RING_DELAY = getRingDelay(); // delay after which we assume ring has stablized
    public static final StorageService instance = new StorageService();

    private static int getRingDelay() {
        String newdelay = Config.getProperty("ring.delay.ms");
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

    public final VersionedValueFactory valueFactory = new VersionedValueFactory(getPartitioner());
    /* This abstraction maintains the token/endpoint metadata information */
    private final TokenMetaData tokenMetaData = new TokenMetaData();
    private final List<IEndpointLifecycleSubscriber> lifecycleSubscribers = new CopyOnWriteArrayList<>();

    /* we bootstrap but do NOT join the ring unless told to do so */
    private boolean isSurveyMode = Boolean.parseBoolean(Config.getProperty("write.survey", "false"));
    private boolean initialized;
    private volatile boolean joined = false;
    /* Are we starting this node in bootstrap mode? */
    private boolean isBootstrapMode;
    private Collection<Token> bootstrapTokens;

    private Thread drainOnShutdown;

    public StorageService() {
    }

    public synchronized void start() throws ConfigurationException {
        initialized = true;

        if (Boolean.parseBoolean(Config.getProperty("load.ring.state", "true"))) {
            logger.info("Loading persisted ring state");
            Multimap<InetAddress, Token> loadedTokens = ClusterMetaData.loadTokens();
            Map<InetAddress, UUID> loadedHostIds = ClusterMetaData.loadHostIds();
            for (InetAddress ep : loadedTokens.keySet()) {
                if (ep.equals(Utils.getBroadcastAddress())) {
                    // entry has been mistakenly added, delete it
                    ClusterMetaData.removeEndpoint(ep);
                } else {
                    tokenMetaData.updateNormalTokens(loadedTokens.get(ep), ep);
                    if (loadedHostIds.containsKey(ep))
                        tokenMetaData.updateHostId(loadedHostIds.get(ep), ep);
                    Gossiper.instance.addSavedEndpoint(ep);
                }
            }
        }

        addShutdownHook();
        prepareToJoin();

        if (Boolean.parseBoolean(Config.getProperty("join.ring", "true"))) {
            joinTokenRing(RING_DELAY);
        } else {
            Collection<Token> tokens = ClusterMetaData.getSavedTokens();
            if (!tokens.isEmpty()) {
                tokenMetaData.updateNormalTokens(tokens, Utils.getBroadcastAddress());
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

            if (DatabaseDescriptor.isReplacing() && !(Boolean.parseBoolean(Config.getProperty("join.ring", "true"))))
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
            getTokenMetaData().updateHostId(localHostId, Utils.getBroadcastAddress());
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
            // LoadBroadcaster.instance.startBroadcasting(); //TODO
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
        // to get schema info from gossip which defeats the purpose. See lealone-4427 for the gory details.
        Set<InetAddress> current = new HashSet<>();
        if (logger.isDebugEnabled())
            logger.debug("Bootstrap variables: {} {} {} {}", DatabaseDescriptor.isAutoBootstrap(), ClusterMetaData
                    .bootstrapInProgress(), ClusterMetaData.bootstrapComplete(), DatabaseDescriptor.getSeeds()
                    .contains(Utils.getBroadcastAddress()));
        if (DatabaseDescriptor.isAutoBootstrap() && !ClusterMetaData.bootstrapComplete()
                && DatabaseDescriptor.getSeeds().contains(Utils.getBroadcastAddress()))
            logger.info("This node will not auto bootstrap because it is configured to be a seed node.");
        if (shouldBootstrap()) {
            if (ClusterMetaData.bootstrapInProgress())
                logger.warn("Detected previous bootstrap failure; retrying");
            else
                ClusterMetaData.setBootstrapState(ClusterMetaData.BootstrapState.IN_PROGRESS);

            if (logger.isDebugEnabled())
                logger.debug("... got ring + schema info");

            if (Boolean.parseBoolean(Config.getProperty("consistent.rangemovement", "true"))
                    && (tokenMetaData.getBootstrapTokens().valueSet().size() > 0 //
                            || tokenMetaData.getLeavingEndpoints().size() > 0 //
                    || tokenMetaData.getMovingEndpoints().size() > 0))
                throw new UnsupportedOperationException("Other bootstrapping/leaving/moving nodes detected, "
                        + "cannot bootstrap while lealone.consistent.rangemovement is true");

            if (!DatabaseDescriptor.isReplacing()) {
                if (tokenMetaData.isMember(Utils.getBroadcastAddress())) {
                    String s = "This node is already a member of the token ring; "
                            + "bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)";
                    throw new UnsupportedOperationException(s);
                }
                setMode(Mode.JOINING, "getting bootstrap token", true);
                bootstrapTokens = BootStrapper.getBootstrapTokens(tokenMetaData);
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
                        InetAddress existing = tokenMetaData.getEndpoint(token);
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
                bootstrapTokens = BootStrapper.getRandomTokens(tokenMetaData, DatabaseDescriptor.getNumTokens());
                if (DatabaseDescriptor.getNumTokens() == 1)
                    logger.warn("Generated random token {}. Random tokens will result in an unbalanced ring; "
                            + "see http://wiki.apache.org/cassandra/Operations", bootstrapTokens);
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
            assert tokenMetaData.sortedTokens().size() > 0;
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
            List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<>(2);
            states.add(Pair.create(ApplicationState.TOKENS, valueFactory.tokens(tokens)));
            states.add(Pair.create(ApplicationState.STATUS, valueFactory.bootstrapping(tokens)));
            Gossiper.instance.addLocalApplicationStates(states);
        } else {
            // Dont set any state for the node which is bootstrapping the existing token...
            tokenMetaData.updateNormalTokens(tokens, Utils.getBroadcastAddress());
            ClusterMetaData.removeEndpoint(DatabaseDescriptor.getReplaceAddress());
        }

        // GossipTask是在另一个线程中异步执行的，
        // 执行bootstrap的线程运行到这里时GossipTask可能还在与seed节点交换信息的过程中，
        // 虽然在执行doShadowRound时与seed节点通信过一次，
        // 但执行完doShadowRound后就在checkForEndpointCollision中马上就调用resetEndpointStateMap把endpointStateMap清空了，
        // 所以seenAnySeed()会返回false，
        // 这里没有像上面那样使用Uninterruptibles.sleepUninterruptibly(RING_DELAY, TimeUnit.MILLISECONDS)，
        // 是不想在执行bootstrap时暂停太久，因为RING_DELAY的默认值是30秒!
        int delay = 0;
        while (true) {
            if (Gossiper.instance.seenAnySeed())
                break;
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            delay += 100;
            if (delay > RING_DELAY)
                throw new IllegalStateException("Unable to contact any seeds!");
        }

        setMode(Mode.JOINING, "Starting to bootstrap...", true);
        new BootStrapper(Utils.getBroadcastAddress(), tokens, tokenMetaData).bootstrap(); // handles token update
        logger.info("Bootstrap completed! for the tokens {}", tokens);
    }

    public void finishBootstrapping() {
        isBootstrapMode = false;
    }

    /** This method updates the local token on disk  */
    private void setTokens(Collection<Token> tokens) {
        if (logger.isDebugEnabled())
            logger.debug("Setting tokens to {}", tokens);
        ClusterMetaData.updateTokens(tokens);
        tokenMetaData.updateNormalTokens(tokens, Utils.getBroadcastAddress());
        Collection<Token> localTokens = getLocalTokens();
        List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<>(2);
        states.add(Pair.create(ApplicationState.TOKENS, valueFactory.tokens(localTokens)));
        states.add(Pair.create(ApplicationState.STATUS, valueFactory.normal(localTokens)));
        Gossiper.instance.addLocalApplicationStates(states);
        setMode(Mode.NORMAL, false);
    }

    public Collection<Range<Token>> getLocalRanges(Schema schema) {
        return getRangesForEndpoint(schema, Utils.getBroadcastAddress());
    }

    public void register(IEndpointLifecycleSubscriber subscriber) {
        lifecycleSubscribers.add(subscriber);
    }

    public void unregister(IEndpointLifecycleSubscriber subscriber) {
        lifecycleSubscribers.remove(subscriber);
    }

    public void startGossiping() {
        if (!initialized) {
            logger.warn("Starting gossip by operator request");
            Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
            initialized = true;
        }
    }

    public void stopGossiping() {
        if (initialized) {
            logger.warn("Stopping gossip by operator request");
            Gossiper.instance.stop();
            initialized = false;
        }
    }

    public boolean isGossipRunning() {
        return Gossiper.instance.isEnabled();
    }

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
            assert tokenMetaData.sortedTokens().size() > 0;
        }
    }

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

    public TokenMetaData getTokenMetaData() {
        return tokenMetaData;
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

    public Map<String, String> getTokenToEndpointMap() {
        Map<Token, InetAddress> mapInetAddress = tokenMetaData.getNormalAndBootstrappingTokenToEndpointMap();
        // in order to preserve tokens in ascending order, we use LinkedHashMap here
        Map<String, String> mapString = new LinkedHashMap<>(mapInetAddress.size());
        List<Token> tokens = new ArrayList<>(mapInetAddress.keySet());
        Collections.sort(tokens);
        for (Token token : tokens) {
            mapString.put(token.toString(), mapInetAddress.get(token).getHostAddress());
        }
        return mapString;
    }

    public String getLocalHostId() {
        return getTokenMetaData().getHostId(Utils.getBroadcastAddress()).toString();
    }

    public Map<String, String> getHostIdMap() {
        Map<String, String> mapOut = new HashMap<>();
        for (Map.Entry<InetAddress, UUID> entry : getTokenMetaData().getEndpointToHostIdMapForReading().entrySet())
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
     * pendingranges set in TokenMetaData; a node in normal mode should instead be part of the token ring.
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

            if (!endpoint.equals(Utils.getBroadcastAddress()))
                updatePeerInfo(endpoint, state, value);
        }
    }

    private void updatePeerInfo(InetAddress endpoint, ApplicationState state, VersionedValue value) {
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

    private void updatePeerInfo(InetAddress endpoint) {
        if (endpoint.equals(Utils.getBroadcastAddress()))
            return;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet()) {
            updatePeerInfo(endpoint, entry.getKey(), entry.getValue());
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
        if (tokenMetaData.isMember(endpoint)) {
            // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
            // isLeaving is true, we have only missed LEFT. Waiting time between completing
            // leave operation and rebootstrapping is relatively short, so the latter is quite
            // common (not enough time for gossip to spread). Therefore we report only the
            // former in the log.
            if (!tokenMetaData.isLeaving(endpoint))
                logger.info("Node {} state jump to bootstrap", endpoint);
            tokenMetaData.removeEndpoint(endpoint);
        }

        tokenMetaData.addBootstrapTokens(tokens, endpoint);

        if (Gossiper.instance.usesHostId(endpoint))
            tokenMetaData.updateHostId(Gossiper.instance.getHostId(endpoint), endpoint);
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

        Set<Token> tokensToUpdateInTokenMetaData = new HashSet<>();
        Set<Token> tokensToUpdateInClusterMetaData = new HashSet<>();
        Set<Token> localTokensToRemove = new HashSet<>();
        Set<InetAddress> endpointsToRemove = new HashSet<>();

        if (logger.isDebugEnabled())
            logger.debug("Node {} state normal, token {}", endpoint, tokens);

        if (tokenMetaData.isMember(endpoint))
            logger.info("Node {} state jump to normal", endpoint);

        updatePeerInfo(endpoint);
        // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see lealone-4300).
        if (Gossiper.instance.usesHostId(endpoint)) {
            UUID hostId = Gossiper.instance.getHostId(endpoint);
            InetAddress existing = tokenMetaData.getEndpointForHostId(hostId);
            if (DatabaseDescriptor.isReplacing()
                    && Gossiper.instance.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress()) != null
                    && (hostId.equals(Gossiper.instance.getHostId(DatabaseDescriptor.getReplaceAddress()))))
                logger.warn("Not updating token metadata for {} because I am replacing it", endpoint);
            else {
                if (existing != null && !existing.equals(endpoint)) {
                    if (existing.equals(Utils.getBroadcastAddress())) {
                        logger.warn("Not updating host ID {} for {} because it's mine", hostId, endpoint);
                        tokenMetaData.removeEndpoint(endpoint);
                        endpointsToRemove.add(endpoint);
                    } else if (Gossiper.instance.compareEndpointStartup(endpoint, existing) > 0) {
                        logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", hostId,
                                existing, endpoint, endpoint);
                        tokenMetaData.removeEndpoint(existing);
                        endpointsToRemove.add(existing);
                        tokenMetaData.updateHostId(hostId, endpoint);
                    } else {
                        logger.warn("Host ID collision for {} between {} and {}; ignored {}", hostId, existing,
                                endpoint, endpoint);
                        tokenMetaData.removeEndpoint(endpoint);
                        endpointsToRemove.add(endpoint);
                    }
                } else
                    tokenMetaData.updateHostId(hostId, endpoint);
            }
        }

        for (final Token token : tokens) {
            // we don't want to update if this node is responsible for the token
            // and it has a later startup time than endpoint.
            InetAddress currentOwner = tokenMetaData.getEndpoint(token);
            if (currentOwner == null) {
                logger.debug("New node {} at token {}", endpoint, token);
                tokensToUpdateInTokenMetaData.add(token);
                tokensToUpdateInClusterMetaData.add(token);
            } else if (endpoint.equals(currentOwner)) {
                // set state back to normal, since the node may have tried to leave, but failed and is now back up
                tokensToUpdateInTokenMetaData.add(token);
                tokensToUpdateInClusterMetaData.add(token);
            } else if (Gossiper.instance.compareEndpointStartup(endpoint, currentOwner) > 0) {
                tokensToUpdateInTokenMetaData.add(token);
                tokensToUpdateInClusterMetaData.add(token);

                // currentOwner is no longer current, endpoint is. Keep track of these moves, because when
                // a host no longer has any tokens, we'll want to remove it.
                Multimap<InetAddress, Token> epToTokenCopy = getTokenMetaData().getEndpointToTokenMapForReading();
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

        tokenMetaData.updateNormalTokens(tokensToUpdateInTokenMetaData, endpoint);
        for (InetAddress ep : endpointsToRemove) {
            removeEndpoint(ep);
            if (DatabaseDescriptor.isReplacing() && DatabaseDescriptor.getReplaceAddress().equals(ep))
                Gossiper.instance.replacementQuarantine(ep); // quarantine locally longer than normally; see
                                                             // Cassandra-8260
        }
        if (!tokensToUpdateInClusterMetaData.isEmpty())
            ClusterMetaData.updateTokens(endpoint, tokensToUpdateInClusterMetaData);
        if (!localTokensToRemove.isEmpty())
            ClusterMetaData.updateLocalTokens(Collections.<Token> emptyList(), localTokensToRemove);

        if (tokenMetaData.isMoving(endpoint)) // if endpoint was moving to a new token
        {
            tokenMetaData.removeFromMoving(endpoint);
            for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onMove(endpoint);
        } else {
            for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onJoinCluster(endpoint);
        }
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
        if (!tokenMetaData.isMember(endpoint)) {
            logger.info("Node {} state jump to leaving", endpoint);
            tokenMetaData.updateNormalTokens(tokens, endpoint);
        } else if (!tokenMetaData.getTokens(endpoint).containsAll(tokens)) {
            logger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
            tokenMetaData.updateNormalTokens(tokens, endpoint);
        }

        // at this point the endpoint is certainly a member with this token, so let's proceed
        // normally
        tokenMetaData.addLeavingEndpoint(endpoint);
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

        tokenMetaData.addMovingEndpoint(token, endpoint);
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
                // drain();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return;
        }
        if (tokenMetaData.isMember(endpoint)) {
            String state = pieces[0];
            Collection<Token> removeTokens = tokenMetaData.getTokens(endpoint);

            if (VersionedValue.REMOVED_TOKEN.equals(state)) {
                excise(removeTokens, endpoint, extractExpireTime(pieces));
            } else if (VersionedValue.REMOVING_TOKEN.equals(state)) {
                if (logger.isDebugEnabled())
                    logger.debug("Tokens {} removed manually (endpoint was {})", removeTokens, endpoint);

                // Note that the endpoint is being removed
                tokenMetaData.addLeavingEndpoint(endpoint);

                // find the endpoint coordinating this removal that we need to notify when we're done
                // String[] coordinator = Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(
                // ApplicationState.REMOVAL_COORDINATOR).value.split(VersionedValue.DELIMITER_STR, -1);
                // UUID hostId = UUID.fromString(coordinator[1]);
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
        tokenMetaData.removeEndpoint(endpoint);
        tokenMetaData.removeBootstrapTokens(tokens);

        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onLeaveCluster(endpoint);
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

    private void addExpireTimeIfFound(InetAddress endpoint, long expireTime) {
        if (expireTime != 0L) {
            Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
        }
    }

    private long extractExpireTime(String[] pieces) {
        return Long.parseLong(pieces[2]);
    }

    @Override
    public void onJoin(InetAddress endpoint, EndpointState epState) {
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet()) {
            onChange(endpoint, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void onAlive(InetAddress endpoint, EndpointState state) {
        if (tokenMetaData.isMember(endpoint)) {
            for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onUp(endpoint);
        }
    }

    @Override
    public void onRemove(InetAddress endpoint) {
        tokenMetaData.removeEndpoint(endpoint);
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
        // TODO 看看硬盘使用情况
        return 0.0;
    }

    public String getLoadString() {
        return FileUtils.stringifyFileSize(getLoad());
    }

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

    public List<String> getTokens() {
        return getTokens(Utils.getBroadcastAddress());
    }

    public List<String> getTokens(String endpoint) throws UnknownHostException {
        return getTokens(InetAddress.getByName(endpoint));
    }

    private List<String> getTokens(InetAddress endpoint) {
        List<String> strTokens = new ArrayList<>();
        for (Token tok : getTokenMetaData().getTokens(endpoint))
            strTokens.add(tok.toString());
        return strTokens;
    }

    public String getReleaseVersion() {
        return Utils.getReleaseVersionString();
    }

    public List<String> getLeavingNodes() {
        return stringify(tokenMetaData.getLeavingEndpoints());
    }

    public List<String> getMovingNodes() {
        List<String> endpoints = new ArrayList<>();

        for (Pair<Token, InetAddress> node : tokenMetaData.getMovingEndpoints()) {
            endpoints.add(node.right.getHostAddress());
        }

        return endpoints;
    }

    public List<String> getJoiningNodes() {
        return stringify(tokenMetaData.getBootstrapTokens().valueSet());
    }

    public List<String> getLiveNodes() {
        return stringify(Gossiper.instance.getLiveMembers());
    }

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

    /**
     * Get all ranges an endpoint is responsible for (by keyspace)
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    Collection<Range<Token>> getRangesForEndpoint(Schema schema, InetAddress ep) {
        return Keyspace.getReplicationStrategy(schema).getAddressRanges().get(ep);
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

    public List<InetAddress> getNaturalEndpoints(Schema schema, ByteBuffer key) {
        return getNaturalEndpoints(schema, getPartitioner().getToken(key));
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param schema the schema
     * @param pos position for which we need to find the endpoint
     * @return the endpoint responsible for this token
     */
    public List<InetAddress> getNaturalEndpoints(Schema schema, RingPosition<?> pos) {
        return Keyspace.getReplicationStrategy(schema).getNaturalEndpoints(pos);
    }

    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param schema the schema
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    public List<InetAddress> getLiveNaturalEndpoints(Schema schema, ByteBuffer key) {
        return getLiveNaturalEndpoints(schema, getPartitioner().decorateKey(key));
    }

    public List<InetAddress> getLiveNaturalEndpoints(Schema schema, RingPosition<?> pos) {
        List<InetAddress> endpoints = Keyspace.getReplicationStrategy(schema).getNaturalEndpoints(pos);
        List<InetAddress> liveEps = new ArrayList<>(endpoints.size());

        for (InetAddress endpoint : endpoints) {
            if (FailureDetector.instance.isAlive(endpoint))
                liveEps.add(endpoint);
        }

        return liveEps;
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

    public void removeNode(String hostIdString) {
    }

    public String getOperationMode() {
        return operationMode.toString();
    }

    public boolean isStarting() {
        return operationMode == Mode.STARTING;
    }
}

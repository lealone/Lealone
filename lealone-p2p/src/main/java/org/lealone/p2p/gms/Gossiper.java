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
package org.lealone.p2p.gms;

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.JVMStabilityInspector;
import org.lealone.common.util.Pair;
import org.lealone.db.async.AsyncPeriodicTask;
import org.lealone.db.async.AsyncTaskHandlerFactory;
import org.lealone.net.NetNode;
import org.lealone.p2p.config.Config;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.net.IAsyncCallback;
import org.lealone.p2p.net.MessageIn;
import org.lealone.p2p.net.MessageOut;
import org.lealone.p2p.net.MessagingService;
import org.lealone.p2p.net.Verb;
import org.lealone.p2p.server.P2pServer;
import org.lealone.p2p.util.Uninterruptibles;
import org.lealone.p2p.util.Utils;

/**
 * This module is responsible for Gossiping information for the local node. This abstraction
 * maintains the list of live and dead nodes. Periodically i.e. every 1 second this module
 * chooses a random node and initiates a round of Gossip with it. A round of Gossip involves 3
 * rounds of messaging. For instance if node A wants to initiate a round of Gossip with node B
 * it starts off by sending node B a GossipDigestSynMessage. Node B on receipt of this message
 * sends node A a GossipDigestAckMessage. On receipt of this message node A sends node B a
 * GossipDigestAck2Message which completes a round of Gossip. This module as and when it hears one
 * of the three above mentioned messages updates the Failure Detector with the liveness information.
 * Upon hearing a GossipShutdownMessage, this module will instantly mark the remote node as down in
 * the Failure Detector.
 */
public class Gossiper implements IFailureDetectionEventListener, GossiperMBean {
    private static final Logger logger = LoggerFactory.getLogger(Gossiper.class);

    // private static final DebuggableScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor(
    // "GossipTasks");
    private static final ReentrantLock taskLock = new ReentrantLock();

    private static final List<String> DEAD_STATES = Arrays.asList(VersionedValue.REMOVING_TOKEN,
            VersionedValue.REMOVED_TOKEN, VersionedValue.STATUS_LEFT, VersionedValue.HIBERNATE);

    private static final int RING_DELAY = getRingDelay(); // delay after which we assume ring has stablized

    private static int getRingDelay() {
        String newDelay = Config.getProperty("ring.delay.ms");
        if (newDelay != null) {
            logger.info("Overriding RING_DELAY to {}ms", newDelay);
            return Integer.parseInt(newDelay);
        } else
            return 30 * 1000;
    }

    // Maximum difference in generation and version values we are willing to accept about a peer
    private static final long MAX_GENERATION_DIFFERENCE = 86400 * 365;
    private static final int QUARANTINE_DELAY = RING_DELAY * 2;
    // half of QUARATINE_DELAY, to ensure justRemovedNodes has enough leeway to prevent re-gossip
    private static final long FAT_CLIENT_TIMEOUT = QUARANTINE_DELAY / 2;
    private static final long A_VERY_LONG_TIME = 259200 * 1000; // 3 days

    public final static int INTERVAL_IN_MILLIS = 1000;

    public final static Gossiper instance = new Gossiper();

    private ScheduledFuture<?> scheduledGossipTask;

    private final Random random = new Random();
    private final Comparator<NetNode> inetcomparator = new Comparator<NetNode>() {
        @Override
        public int compare(NetNode addr1, NetNode addr2) {
            return addr1.compareTo(addr2);
        }
    };

    /* subscribers for interest in NodeState change */
    private final List<INodeStateChangeSubscriber> subscribers = new CopyOnWriteArrayList<>();

    /* live member set */
    private final Set<NetNode> liveNodes = new ConcurrentSkipListSet<>(inetcomparator);

    /* unreachable member set */
    private final Map<NetNode, Long> unreachableNodes = new ConcurrentHashMap<>();

    /* initial seeds for joining the cluster */
    private final Set<NetNode> seeds = new ConcurrentSkipListSet<>(inetcomparator);

    /* map where key is the node and value is the state associated with the node */
    final ConcurrentMap<NetNode, NodeState> nodeStateMap = new ConcurrentHashMap<>();

    /* map where key is node and value is timestamp when this node was removed from
     * gossip. We will ignore any gossip regarding these nodes for QUARANTINE_DELAY time
     * after removal to prevent nodes from falsely reincarnating during the time when removal
     * gossip gets propagated to all nodes */
    private final Map<NetNode, Long> justRemovedNodes = new ConcurrentHashMap<>();

    private final Map<NetNode, Long> expireTimeNodeMap = new ConcurrentHashMap<>();

    private boolean inShadowRound = false;

    // private volatile long lastProcessedMessageAt = System.currentTimeMillis();

    private class GossipTask implements AsyncPeriodicTask {
        @Override
        public void run() {
            try {
                taskLock.lock();

                /* Update the local heartbeat counter. */
                nodeStateMap.get(ConfigDescriptor.getLocalNode()).getHeartBeatState().updateHeartBeat();
                if (logger.isTraceEnabled())
                    logger.trace("My heartbeat is now {}", nodeStateMap.get(ConfigDescriptor.getLocalNode())
                            .getHeartBeatState().getHeartBeatVersion());
                final List<GossipDigest> gDigests = new ArrayList<>();
                Gossiper.instance.makeRandomGossipDigest(gDigests);

                if (gDigests.size() > 0) {
                    GossipDigestSyn digestSynMessage = new GossipDigestSyn(ConfigDescriptor.getClusterName(), gDigests);
                    MessageOut<GossipDigestSyn> message = new MessageOut<>(Verb.GOSSIP_DIGEST_SYN, digestSynMessage);
                    /* Gossip to some random live member */
                    boolean gossipedToSeed = doGossipToLiveMember(message);

                    /* Gossip to some unreachable member with some probability to check if he is back up */
                    doGossipToUnreachableMember(message);

                    /* Gossip to a seed if we did not do so above, or we have seen less nodes
                       than there are seeds.  This prevents partitions where each group of nodes
                       is only gossiping to a subset of the seeds.
                    
                       The most straightforward check would be to check that all the seeds have been
                       verified either as live or unreachable.  To avoid that computation each round,
                       we reason that:
                    
                       either all the live nodes are seeds, in which case non-seeds that come online
                       will introduce themselves to a member of the ring by definition,
                    
                       or there is at least one non-seed node in the list, in which case eventually
                       someone will gossip to it, and then do a gossip to a random seed from the
                       gossipedToSeed check.
                    
                       See Cassandra-150 for more exposition. */
                    if (!gossipedToSeed || liveNodes.size() < seeds.size())
                        doGossipToSeed(message);

                    doStatusCheck();
                }
            } catch (Exception e) {
                JVMStabilityInspector.inspectThrowable(e);
                logger.error("Gossip error", e);
            } finally {
                taskLock.unlock();
            }
        }
    }

    private Gossiper() {
        // register with the Failure Detector for receiving Failure detector events
        FailureDetector.instance.registerFailureDetectionEventListener(this);

        // Register this instance with JMX
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(Utils.getJmxObjectName("Gossiper")));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void start(int generationNumber) {
        start(generationNumber, new HashMap<ApplicationState, VersionedValue>());
    }

    /**
     * Start the gossiper with the generation number, preloading the map of application states before starting
     */
    public void start(int generationNbr, Map<ApplicationState, VersionedValue> preloadLocalStates) {
        buildSeedsList();
        // initialize the heartbeat state for this localNode
        maybeInitializeLocalState(generationNbr);
        NodeState localState = nodeStateMap.get(ConfigDescriptor.getLocalNode());
        for (Map.Entry<ApplicationState, VersionedValue> entry : preloadLocalStates.entrySet())
            localState.addApplicationState(entry.getKey(), entry.getValue());

        // notify snitches that Gossiper is about to start
        ConfigDescriptor.getNodeSnitch().gossiperStarting();
        if (logger.isTraceEnabled())
            logger.trace("gossip started with generation {}", localState.getHeartBeatState().getGeneration());

        // scheduledGossipTask = executor.scheduleWithFixedDelay(new GossipTask(), Gossiper.INTERVAL_IN_MILLIS,
        // Gossiper.INTERVAL_IN_MILLIS, TimeUnit.MILLISECONDS);

        AsyncTaskHandlerFactory.getAsyncTaskHandler().scheduleWithFixedDelay(new GossipTask(),
                Gossiper.INTERVAL_IN_MILLIS, Gossiper.INTERVAL_IN_MILLIS, TimeUnit.MILLISECONDS);
    }

    private void buildSeedsList() {
        NetNode local = ConfigDescriptor.getLocalNode();
        for (NetNode seed : ConfigDescriptor.getSeeds()) {
            if (!seed.equals(local)) {
                seeds.add(seed);
            }
        }
    }

    // initialize local HB state if needed, i.e., if gossiper has never been started before.
    private void maybeInitializeLocalState(int generationNbr) {
        HeartBeatState hbState = new HeartBeatState(generationNbr);
        NodeState localState = new NodeState(hbState);
        nodeStateMap.putIfAbsent(ConfigDescriptor.getLocalNode(), localState);
    }

    /**
     * The gossip digest is built based on randomization
     * rather than just looping through the collection of live nodes.
     *
     * @param gDigests list of Gossip Digests.
     */
    private void makeRandomGossipDigest(List<GossipDigest> gDigests) {
        NodeState epState;
        int generation = 0;
        int maxVersion = 0;

        // local epstate will be part of nodeStateMap
        List<NetNode> nodes = new ArrayList<>(nodeStateMap.keySet());
        Collections.shuffle(nodes, random);
        for (NetNode node : nodes) {
            epState = nodeStateMap.get(node);
            if (epState != null) {
                generation = epState.getHeartBeatState().getGeneration();
                maxVersion = getMaxNodeStateVersion(epState);
            }
            gDigests.add(new GossipDigest(node, generation, maxVersion));
        }

        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (GossipDigest gDigest : gDigests) {
                sb.append(gDigest);
                sb.append(" ");
            }
            logger.trace("Gossip Digests are : {}", sb);
        }
    }

    /* Sends a Gossip message to a live member and returns true if the recipient was a seed */
    private boolean doGossipToLiveMember(MessageOut<GossipDigestSyn> message) {
        if (liveNodes.isEmpty())
            return false;
        return sendGossip(message, liveNodes);
    }

    /* Sends a Gossip message to an unreachable member */
    private void doGossipToUnreachableMember(MessageOut<GossipDigestSyn> message) {
        double liveNodeCount = liveNodes.size();
        double unreachableNodeCount = unreachableNodes.size();
        if (unreachableNodeCount > 0) {
            /* based on some probability */
            double prob = unreachableNodeCount / (liveNodeCount + 1);
            double randDbl = random.nextDouble();
            if (randDbl < prob)
                sendGossip(message, unreachableNodes.keySet());
        }
    }

    /* Gossip to a seed for facilitating partition healing */
    private void doGossipToSeed(MessageOut<GossipDigestSyn> prod) {
        int size = seeds.size();
        if (size > 0) {
            if (size == 1 && seeds.contains(ConfigDescriptor.getLocalNode())) {
                return;
            }

            if (liveNodes.isEmpty()) {
                sendGossip(prod, seeds);
            } else {
                /* Gossip with the seed with some probability. */
                double probability = seeds.size() / (double) (liveNodes.size() + unreachableNodes.size());
                double randDbl = random.nextDouble();
                if (randDbl <= probability)
                    sendGossip(prod, seeds);
            }
        }
    }

    /**
     * Returns true if the chosen target was also a seed. False otherwise
     *
     * @param message
     * @param epSet   a set of node from which a random node is chosen.
     * @return true if the chosen node is also a seed.
     */
    private boolean sendGossip(MessageOut<GossipDigestSyn> message, Set<NetNode> epSet) {
        List<NetNode> liveNodes = new ArrayList<>(epSet);

        int size = liveNodes.size();
        if (size < 1)
            return false;
        /* Generate a random number from 0 -> size */
        int index = (size == 1) ? 0 : random.nextInt(size);
        NetNode to = liveNodes.get(index);
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestSyn to {} ...", to);
        MessagingService.instance().sendOneWay(message, to);
        return seeds.contains(to);
    }

    private void doStatusCheck() {
        if (logger.isTraceEnabled())
            logger.trace("Performing status check ...");

        long now = System.currentTimeMillis();
        long nowNano = System.nanoTime();

        // long pending = ((MetricsEnabledThreadPoolExecutor) StageManager.getStage(Stage.GOSSIP)).getPendingTasks();
        // if (pending > 0 && lastProcessedMessageAt < now - 1000) {
        // // if some new messages just arrived, give the executor some time to work on them
        // Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        //
        // // still behind? something's broke
        // if (lastProcessedMessageAt < now - 1000) {
        // logger.warn("Gossip stage has {} pending tasks; skipping status check (no nodes will be marked down)",
        // pending);
        // return;
        // }
        // }

        Set<NetNode> eps = nodeStateMap.keySet();
        for (NetNode node : eps) {
            if (node.equals(ConfigDescriptor.getLocalNode()))
                continue;

            FailureDetector.instance.interpret(node);
            NodeState epState = nodeStateMap.get(node);
            if (epState != null) {
                // check if this is a fat client. fat clients are removed automatically from
                // gossip after FatClientTimeout. Do not remove dead states here.
                if (isGossipOnlyMember(node) && !justRemovedNodes.containsKey(node)
                        && TimeUnit.NANOSECONDS.toMillis(nowNano - epState.getUpdateTimestamp()) > FAT_CLIENT_TIMEOUT) {
                    logger.info("FatClient {} has been silent for {}ms, removing from gossip", node,
                            FAT_CLIENT_TIMEOUT);
                    removeNode(node); // will put it in justRemovedNodes to respect quarantine delay
                    evictFromMembership(node); // can get rid of the state immediately
                }

                // check for dead state removal
                long expireTime = getExpireTimeForNode(node);
                if (!epState.isAlive() && (now > expireTime)
                        && (!P2pServer.instance.getTopologyMetaData().isMember(node))) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("time is expiring for node : {} ({})", node, expireTime);
                    }
                    evictFromMembership(node);
                }
            }
        }

        if (!justRemovedNodes.isEmpty()) {
            for (Entry<NetNode, Long> entry : justRemovedNodes.entrySet()) {
                if ((now - entry.getValue()) > QUARANTINE_DELAY) {
                    if (logger.isDebugEnabled())
                        logger.debug("{} elapsed, {} gossip quarantine over", QUARANTINE_DELAY, entry.getKey());
                    justRemovedNodes.remove(entry.getKey());
                }
            }
        }
    }

    public void setLastProcessedMessageAt(long timeInMillis) {
        // this.lastProcessedMessageAt = timeInMillis;
    }

    /**
     * Register for interesting state changes.
     *
     * @param subscriber module which implements the INodeStateChangeSubscriber
     */
    public void register(INodeStateChangeSubscriber subscriber) {
        subscribers.add(subscriber);
    }

    /**
     * Unregister interest for state changes.
     *
     * @param subscriber module which implements the INodeStateChangeSubscriber
     */
    public void unregister(INodeStateChangeSubscriber subscriber) {
        subscribers.remove(subscriber);
    }

    public Set<NetNode> getLiveMembers() {
        Set<NetNode> liveMembers = new HashSet<>(liveNodes);
        if (!liveMembers.contains(ConfigDescriptor.getLocalNode()))
            liveMembers.add(ConfigDescriptor.getLocalNode());
        return liveMembers;
    }

    /**
     * @return a list of unreachable gossip participants, including fat clients
     */
    public Set<NetNode> getUnreachableMembers() {
        return unreachableNodes.keySet();
    }

    // /**
    // * @return a list of unreachable token owners
    // */
    // public Set<NetNode> getUnreachableTokenOwners() {
    // Set<NetNode> tokenOwners = new HashSet<>();
    // for (NetNode node : unreachableNodes.keySet()) {
    // if (P2pServer.instance.getTopologyMetaData().isMember(node))
    // tokenOwners.add(node);
    // }
    //
    // return tokenOwners;
    // }

    /**
     * This method is part of IFailureDetectionEventListener interface. This is invoked
     * by the Failure Detector when it convicts an end point.
     *
     * @param node end point that is convicted.
     */
    @Override
    public void convict(NetNode node, double phi) {
        NodeState epState = nodeStateMap.get(node);
        if (epState == null)
            return;
        if (epState.isAlive() && !isDeadState(epState)) {
            markDead(node, epState);
        } else
            epState.markDead();
    }

    /**
     * Return either: the greatest heartbeat or application state
     *
     * @param epState
     * @return
     */
    int getMaxNodeStateVersion(NodeState epState) {
        int maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
        for (VersionedValue value : epState.getApplicationStateMap().values())
            maxVersion = Math.max(maxVersion, value.version);
        return maxVersion;
    }

    /**
     * Removes the node from gossip completely
     *
     * @param node node to be removed from the current membership.
     */
    private void evictFromMembership(NetNode node) {
        unreachableNodes.remove(node);
        nodeStateMap.remove(node);
        expireTimeNodeMap.remove(node);
        quarantineNode(node);
        if (logger.isDebugEnabled())
            logger.debug("evicting {} from gossip", node);
    }

    /**
     * Removes the node from Gossip but retains node state
     */
    public void removeNode(NetNode node) {
        // do subscribers first so anything in the subscriber that depends on gossiper state won't get confused
        for (INodeStateChangeSubscriber subscriber : subscribers)
            subscriber.onRemove(node);

        if (seeds.contains(node)) {
            buildSeedsList();
            seeds.remove(node);
            logger.info("removed {} from seeds, updated seeds list = {}", node, seeds);
        }

        liveNodes.remove(node);
        unreachableNodes.remove(node);
        // do not remove nodeState until the quarantine expires
        FailureDetector.instance.remove(node);
        MessagingService.instance().removeVersion(node);
        quarantineNode(node);
        MessagingService.instance().removeConnection(node);
        if (logger.isDebugEnabled())
            logger.debug("removing node {}", node);
    }

    // 在启动阶段，如果种子节点没有启动成功并且又没有其他活跃节点时，如果无法连接到种子节点，先不删除
    public boolean tryRemoveNode(NetNode node) {
        if (seeds.contains(node) && liveNodes.isEmpty()) {
            return false;
        } else {
            removeNode(node);
            return true;
        }
    }

    /**
     * Quarantines the node for QUARANTINE_DELAY
     *
     * @param node
     */
    private void quarantineNode(NetNode node) {
        quarantineNode(node, System.currentTimeMillis());
    }

    /**
     * Quarantines the node until quarantineExpiration + QUARANTINE_DELAY
     *
     * @param node
     * @param quarantineExpiration
     */
    private void quarantineNode(NetNode node, long quarantineExpiration) {
        justRemovedNodes.put(node, quarantineExpiration);
    }

    /**
     * Quarantine node specifically for replacement purposes.
     * @param node
     */
    public void replacementQuarantine(NetNode node) {
        // remember, quarantineNode will effectively already add QUARANTINE_DELAY, so this is 2x
        quarantineNode(node, System.currentTimeMillis() + QUARANTINE_DELAY);
    }

    /**
     * Remove the Node and evict immediately, to avoid gossiping about this node.
     * This should only be called when a token is taken over by a new IP address.
     *
     * @param node The node that has been replaced
     */
    // TODO 没用到，考虑删除
    public void replacedNode(NetNode node) {
        removeNode(node);
        evictFromMembership(node);
        replacementQuarantine(node);
    }

    /**
     * This method will begin removing an existing node from the cluster by spoofing its state
     * This should never be called unless this coordinator has had 'removenode' invoked
     *
     * @param node    - the node being removed
     * @param hostId      - the ID of the host being removed
     * @param localHostId - my own host ID for replication coordination
     */
    // TODO 没用到，考虑删除
    public void advertiseRemoving(NetNode node, UUID hostId, UUID localHostId) {
        NodeState epState = nodeStateMap.get(node);
        // remember this node's generation
        int generation = epState.getHeartBeatState().getGeneration();
        logger.info("Removing host: {}", hostId);
        logger.info("Sleeping for {}ms to ensure {} does not change", RING_DELAY, node);
        Uninterruptibles.sleepUninterruptibly(RING_DELAY, TimeUnit.MILLISECONDS);
        // make sure it did not change
        epState = nodeStateMap.get(node);
        if (epState.getHeartBeatState().getGeneration() != generation)
            throw new RuntimeException("Node " + node + " generation changed while trying to remove it");
        // update the other node's generation to mimic it as if it had changed it itself
        logger.info("Advertising removal for {}", node);
        epState.updateTimestamp(); // make sure we don't evict it too soon
        epState.getHeartBeatState().forceNewerGenerationUnsafe();
        epState.addApplicationState(ApplicationState.STATUS, P2pServer.valueFactory.removingNonlocal(hostId));
        epState.addApplicationState(ApplicationState.REMOVAL_COORDINATOR,
                P2pServer.valueFactory.removalCoordinator(localHostId));
        nodeStateMap.put(node, epState);
    }

    /**
     * Do not call this method unless you know what you are doing.
     * It will try extremely hard to obliterate any node from the ring,
     * even if it does not know about it.
     *
     * @param address
     * @throws UnknownHostException
     */
    @Override
    public void assassinateNode(String address) throws UnknownHostException {
        NetNode node = NetNode.getByName(address);
        NodeState epState = nodeStateMap.get(node);
        logger.warn("Assassinating {} via gossip", node);

        if (epState == null) {
            epState = new NodeState(new HeartBeatState((int) ((System.currentTimeMillis() + 60000) / 1000), 9999));
        } else {
            int generation = epState.getHeartBeatState().getGeneration();
            int heartbeat = epState.getHeartBeatState().getHeartBeatVersion();
            logger.info("Sleeping for {}ms to ensure {} does not change", RING_DELAY, node);
            Uninterruptibles.sleepUninterruptibly(RING_DELAY, TimeUnit.MILLISECONDS);
            // make sure it did not change
            NodeState newState = nodeStateMap.get(node);
            if (newState == null)
                logger.warn("Node {} disappeared while trying to assassinate, continuing anyway", node);
            else if (newState.getHeartBeatState().getGeneration() != generation)
                throw new RuntimeException(
                        "Node still alive: " + node + " generation changed while trying to assassinate it");
            else if (newState.getHeartBeatState().getHeartBeatVersion() != heartbeat)
                throw new RuntimeException(
                        "Node still alive: " + node + " heartbeat changed while trying to assassinate it");
            epState.updateTimestamp(); // make sure we don't evict it too soon
            epState.getHeartBeatState().forceNewerGenerationUnsafe();
        }

        // do not pass go, do not collect 200 dollars, just gtfo
        epState.addApplicationState(ApplicationState.STATUS, P2pServer.valueFactory.left(null, computeExpireTime()));
        handleMajorStateChange(node, epState);
        Uninterruptibles.sleepUninterruptibly(INTERVAL_IN_MILLIS * 4, TimeUnit.MILLISECONDS);
        logger.warn("Finished assassinating {}", node);
    }

    @Override
    public long getNodeDowntime(String address) throws UnknownHostException {
        NetNode ep = NetNode.getByName(address);
        Long downtime = unreachableNodes.get(ep);
        if (downtime != null)
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - downtime);
        else
            return 0L;
    }

    @Override
    public int getCurrentGenerationNumber(String address) throws UnknownHostException {
        NetNode ep = NetNode.getByName(address);
        return nodeStateMap.get(ep).getHeartBeatState().getGeneration();
    }

    public boolean isKnownNode(NetNode node) {
        return nodeStateMap.containsKey(node);
    }

    public boolean isGossipOnlyMember(NetNode node) {
        NodeState epState = nodeStateMap.get(node);
        if (epState == null) {
            return false;
        }
        return !isDeadState(epState) && !P2pServer.instance.getTopologyMetaData().isMember(node);
    }

    protected long getExpireTimeForNode(NetNode node) {
        /* default expireTime is aVeryLongTime */
        Long storedTime = expireTimeNodeMap.get(node);
        return storedTime == null ? computeExpireTime() : storedTime;
    }

    public NodeState getNodeState(NetNode ep) {
        return nodeStateMap.get(ep);
    }

    // removes ALL node states; should only be called after shadow gossip
    public void resetNodeStateMap() {
        nodeStateMap.clear();
        unreachableNodes.clear();
        liveNodes.clear();
    }

    public Set<Entry<NetNode, NodeState>> getNodeStates() {
        return nodeStateMap.entrySet();
    }

    public boolean usesHostId(NetNode node) {
        if (MessagingService.instance().knowsVersion(node))
            return true;
        else if (getNodeState(node).getApplicationState(ApplicationState.NET_VERSION) != null)
            return true;
        return false;
    }

    public String getHostId(NetNode node) {
        if (!usesHostId(node))
            throw new RuntimeException("Host " + node + " does not use new-style tokens!");
        return getNodeState(node).getApplicationState(ApplicationState.HOST_ID).value;
    }

    public NetNode getTcpNode(NetNode node) {
        if (!usesHostId(node))
            throw new RuntimeException("Host " + node + " does not use new-style tokens!");
        return NetNode.createP2P(getNodeState(node).getApplicationState(ApplicationState.TCP_NODE).value);
    }

    public String getLoad(NetNode node) {
        if (!usesHostId(node))
            throw new RuntimeException("Host " + node + " does not use new-style tokens!");
        return getNodeState(node).getApplicationState(ApplicationState.LOAD).value;
    }

    NodeState getStateForVersionBiggerThan(NetNode forNode, int version) {
        NodeState epState = nodeStateMap.get(forNode);
        NodeState reqdNodeState = null;

        if (epState != null) {
            /*
             * Here we try to include the Heart Beat state only if it is
             * greater than the version passed in. It might happen that
             * the heart beat version maybe lesser than the version passed
             * in and some application state has a version that is greater
             * than the version passed in. In this case we also send the old
             * heart beat and throw it away on the receiver if it is redundant.
            */
            int localHbVersion = epState.getHeartBeatState().getHeartBeatVersion();
            if (localHbVersion > version) {
                reqdNodeState = new NodeState(epState.getHeartBeatState());
                if (logger.isTraceEnabled())
                    logger.trace("local heartbeat version {} greater than {} for {}", localHbVersion, version, forNode);
            }
            /* Accumulate all application states whose versions are greater than "version" variable */
            for (Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet()) {
                VersionedValue value = entry.getValue();
                if (value.version > version) {
                    if (reqdNodeState == null) {
                        reqdNodeState = new NodeState(epState.getHeartBeatState());
                    }
                    final ApplicationState key = entry.getKey();
                    if (logger.isTraceEnabled())
                        logger.trace("Adding state {}: {}", key, value.value);
                    reqdNodeState.addApplicationState(key, value);
                }
            }
        }
        return reqdNodeState;
    }

    /**
     * determine which node started up earlier
     */
    public int compareNodeStartup(NetNode addr1, NetNode addr2) {
        NodeState ep1 = getNodeState(addr1);
        NodeState ep2 = getNodeState(addr2);
        assert ep1 != null && ep2 != null;
        return ep1.getHeartBeatState().getGeneration() - ep2.getHeartBeatState().getGeneration();
    }

    void notifyFailureDetector(Map<NetNode, NodeState> remoteEpStateMap) {
        for (Entry<NetNode, NodeState> entry : remoteEpStateMap.entrySet()) {
            notifyFailureDetector(entry.getKey(), entry.getValue());
        }
    }

    void notifyFailureDetector(NetNode node, NodeState remoteNodeState) {
        NodeState localNodeState = nodeStateMap.get(node);
        /*
         * If the local node state exists then report to the FD only
         * if the versions workout.
        */
        if (localNodeState != null) {
            IFailureDetector fd = FailureDetector.instance;
            int localGeneration = localNodeState.getHeartBeatState().getGeneration();
            int remoteGeneration = remoteNodeState.getHeartBeatState().getGeneration();
            if (remoteGeneration > localGeneration) {
                localNodeState.updateTimestamp();
                // this node was dead and the generation changed, this indicates a reboot, or possibly a takeover
                // we will clean the fd intervals for it and relearn them
                if (!localNodeState.isAlive()) {
                    logger.debug("Clearing interval times for {} due to generation change", node);
                    fd.remove(node);
                }
                fd.report(node);
                return;
            }

            if (remoteGeneration == localGeneration) {
                int localVersion = getMaxNodeStateVersion(localNodeState);
                int remoteVersion = remoteNodeState.getHeartBeatState().getHeartBeatVersion();
                if (remoteVersion > localVersion) {
                    localNodeState.updateTimestamp();
                    // just a version change, report to the fd
                    fd.report(node);
                }
            }
        }
    }

    private void markAlive(final NetNode addr, final NodeState localState) {
        localState.markDead();

        MessageOut<EchoMessage> echoMessage = new MessageOut<>(Verb.ECHO, new EchoMessage());
        logger.trace("Sending a EchoMessage to {}", addr);
        IAsyncCallback<Void> echoHandler = new IAsyncCallback<Void>() {
            @Override
            public boolean isLatencyForSnitch() {
                return false;
            }

            @Override
            public void response(MessageIn<Void> msg) {
                realMarkAlive(addr, localState);
            }
        };
        MessagingService.instance().sendRR(echoMessage, addr, echoHandler);
    }

    private void realMarkAlive(final NetNode addr, final NodeState localState) {
        if (logger.isTraceEnabled())
            logger.trace("marking as alive {}", addr);
        localState.markAlive();
        localState.updateTimestamp(); // prevents doStatusCheck from racing us and evicting if it was down >
                                      // aVeryLongTime
        liveNodes.add(addr);
        unreachableNodes.remove(addr);
        expireTimeNodeMap.remove(addr);
        logger.debug("removing expire time for node : {}", addr);
        logger.info("Node {} is now UP", addr);
        for (INodeStateChangeSubscriber subscriber : subscribers)
            subscriber.onAlive(addr, localState);
        if (logger.isTraceEnabled())
            logger.trace("Notified {}", subscribers);
    }

    private void markDead(NetNode addr, NodeState localState) {
        if (logger.isTraceEnabled())
            logger.trace("marking as down {}", addr);
        localState.markDead();
        liveNodes.remove(addr);
        unreachableNodes.put(addr, System.nanoTime());
        logger.info("Node {} is now DOWN", addr);
        for (INodeStateChangeSubscriber subscriber : subscribers)
            subscriber.onDead(addr, localState);
        if (logger.isTraceEnabled())
            logger.trace("Notified {}", subscribers);
    }

    /**
     * This method is called whenever there is a "big" change in ep state (a generation change for a known node).
     *
     * @param ep      node
     * @param epState NodeState for the node
     */
    private void handleMajorStateChange(NetNode ep, NodeState epState) {
        if (!isDeadState(epState)) {
            if (nodeStateMap.get(ep) != null)
                logger.info("Node {} has restarted, now UP", ep);
            else
                logger.info("Node {} is now part of the cluster", ep);
        }
        if (logger.isTraceEnabled())
            logger.trace("Adding node state for {}", ep);
        nodeStateMap.put(ep, epState);

        // the node restarted: it is up to the subscriber to take whatever action is necessary
        for (INodeStateChangeSubscriber subscriber : subscribers)
            subscriber.onRestart(ep, epState);

        if (!isDeadState(epState))
            markAlive(ep, epState);
        else {
            logger.debug("Not marking {} alive due to dead state", ep);
            markDead(ep, epState);
        }
        for (INodeStateChangeSubscriber subscriber : subscribers)
            subscriber.onJoin(ep, epState);
    }

    public boolean isDeadState(NodeState epState) {
        if (epState.getApplicationState(ApplicationState.STATUS) == null)
            return false;
        String value = epState.getApplicationState(ApplicationState.STATUS).value;
        String[] pieces = value.split(VersionedValue.DELIMITER_STR, -1);
        assert (pieces.length > 0);
        String state = pieces[0];
        for (String deadstate : DEAD_STATES) {
            if (state.equals(deadstate))
                return true;
        }
        return false;
    }

    void applyStateLocally(Map<NetNode, NodeState> epStateMap) {
        for (Entry<NetNode, NodeState> entry : epStateMap.entrySet()) {
            NetNode ep = entry.getKey();
            if (ep.equals(ConfigDescriptor.getLocalNode()) && !isInShadowRound())
                continue;
            if (justRemovedNodes.containsKey(ep)) {
                if (logger.isTraceEnabled())
                    logger.trace("Ignoring gossip for {} because it is quarantined", ep);
                continue;
            }

            NodeState localEpStatePtr = nodeStateMap.get(ep);
            NodeState remoteState = entry.getValue();
            /*
                If state does not exist just add it. If it does then add it if the remote generation is greater.
                If there is a generation tie, attempt to break it by heartbeat version.
            */
            if (localEpStatePtr != null) {
                int localGeneration = localEpStatePtr.getHeartBeatState().getGeneration();
                int remoteGeneration = remoteState.getHeartBeatState().getGeneration();
                if (logger.isTraceEnabled())
                    logger.trace("{} local generation {}, remote generation {}", ep, localGeneration, remoteGeneration);

                if (localGeneration != 0 && remoteGeneration > localGeneration + MAX_GENERATION_DIFFERENCE) {
                    // assume some peer has corrupted memory
                    // and is broadcasting an unbelievable generation about another peer (or itself)
                    logger.warn(
                            "received an invalid gossip generation for peer {}; "
                                    + "local generation = {}, received generation = {}",
                            ep, localGeneration, remoteGeneration);
                } else if (remoteGeneration > localGeneration) {
                    if (logger.isTraceEnabled())
                        logger.trace("Updating heartbeat state generation to {} from {} for {}", remoteGeneration,
                                localGeneration, ep);
                    // major state change will handle the update by inserting the remote state directly
                    handleMajorStateChange(ep, remoteState);
                } else if (remoteGeneration == localGeneration) // generation has not changed, apply new states
                {
                    /* find maximum state */
                    int localMaxVersion = getMaxNodeStateVersion(localEpStatePtr);
                    int remoteMaxVersion = getMaxNodeStateVersion(remoteState);
                    if (remoteMaxVersion > localMaxVersion) {
                        // apply states, but do not notify since there is no major change
                        applyNewStates(ep, localEpStatePtr, remoteState);
                    } else if (logger.isTraceEnabled())
                        logger.trace("Ignoring remote version {} <= {} for {}", remoteMaxVersion, localMaxVersion, ep);
                    if (!localEpStatePtr.isAlive() && !isDeadState(localEpStatePtr)) // unless of course, it was dead
                        markAlive(ep, localEpStatePtr);
                } else {
                    if (logger.isTraceEnabled())
                        logger.trace("Ignoring remote generation {} < {}", remoteGeneration, localGeneration);
                }
            } else {
                // this is a new node, report it to the FD in case it is the first time we are seeing it AND it's not
                // alive
                FailureDetector.instance.report(ep);
                handleMajorStateChange(ep, remoteState);
            }
        }
    }

    private void applyNewStates(NetNode addr, NodeState localState, NodeState remoteState) {
        // don't assert here, since if the node restarts the version will go back to zero
        int oldVersion = localState.getHeartBeatState().getHeartBeatVersion();

        localState.setHeartBeatState(remoteState.getHeartBeatState());
        if (logger.isTraceEnabled())
            logger.trace("Updating heartbeat state version to {} from {} for {} ...",
                    localState.getHeartBeatState().getHeartBeatVersion(), oldVersion, addr);

        // we need to make two loops here, one to apply, then another to notify,
        // this way all states in an update are present and current when the notifications are received
        for (Entry<ApplicationState, VersionedValue> remoteEntry : remoteState.getApplicationStateMap().entrySet()) {
            ApplicationState remoteKey = remoteEntry.getKey();
            VersionedValue remoteValue = remoteEntry.getValue();

            assert remoteState.getHeartBeatState().getGeneration() == localState.getHeartBeatState().getGeneration();
            localState.addApplicationState(remoteKey, remoteValue);
        }
        for (Entry<ApplicationState, VersionedValue> remoteEntry : remoteState.getApplicationStateMap().entrySet()) {
            doOnChangeNotifications(addr, remoteEntry.getKey(), remoteEntry.getValue());
        }
    }

    // notify that a local application state is going to change (doesn't get triggered for remote changes)
    private void doBeforeChangeNotifications(NetNode addr, NodeState epState, ApplicationState apState,
            VersionedValue newValue) {
        for (INodeStateChangeSubscriber subscriber : subscribers) {
            subscriber.beforeChange(addr, epState, apState, newValue);
        }
    }

    // notify that an application state has changed
    private void doOnChangeNotifications(NetNode addr, ApplicationState state, VersionedValue value) {
        for (INodeStateChangeSubscriber subscriber : subscribers) {
            subscriber.onChange(addr, state, value);
        }
    }

    /* Request all the state for the node in the gDigest */
    private void requestAll(GossipDigest gDigest, List<GossipDigest> deltaGossipDigestList, int remoteGeneration) {
        /* We are here since we have no data for this node locally so request everthing. */
        deltaGossipDigestList.add(new GossipDigest(gDigest.getNode(), remoteGeneration, 0));
        if (logger.isTraceEnabled())
            logger.trace("requestAll for {}", gDigest.getNode());
    }

    /* Send all the data with version greater than maxRemoteVersion */
    private void sendAll(GossipDigest gDigest, Map<NetNode, NodeState> deltaEpStateMap, int maxRemoteVersion) {
        NodeState localEpStatePtr = getStateForVersionBiggerThan(gDigest.getNode(), maxRemoteVersion);
        if (localEpStatePtr != null)
            deltaEpStateMap.put(gDigest.getNode(), localEpStatePtr);
    }

    /*
        This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
        and the delta state are built up.
    */
    void examineGossiper(List<GossipDigest> gDigestList, List<GossipDigest> deltaGossipDigestList,
            Map<NetNode, NodeState> deltaEpStateMap) {
        if (gDigestList.isEmpty()) {
            /* we've been sent a *completely* empty syn, 
             * which should normally never happen since an node will at least send a syn with itself.
             * If this is happening then the node is attempting shadow gossip, and we should reply with everything we know.
             */
            if (logger.isDebugEnabled())
                logger.debug("Shadow request received, adding all states");
            for (Map.Entry<NetNode, NodeState> entry : nodeStateMap.entrySet()) {
                gDigestList.add(new GossipDigest(entry.getKey(), 0, 0));
            }
        }
        for (GossipDigest gDigest : gDigestList) {
            int remoteGeneration = gDigest.getGeneration();
            int maxRemoteVersion = gDigest.getMaxVersion();
            /* Get state associated with the end point in digest */
            NodeState epStatePtr = nodeStateMap.get(gDigest.getNode());
            /*
                Here we need to fire a GossipDigestAckMessage. If we have some data associated with this node locally
                then we follow the "if" path of the logic. If we have absolutely nothing for this node we need to
                request all the data for this node.
            */
            if (epStatePtr != null) {
                int localGeneration = epStatePtr.getHeartBeatState().getGeneration();
                /* get the max version of all keys in the state associated with this node */
                int maxLocalVersion = getMaxNodeStateVersion(epStatePtr);
                if (remoteGeneration == localGeneration && maxRemoteVersion == maxLocalVersion)
                    continue;

                if (remoteGeneration > localGeneration) {
                    /* we request everything from the gossiper */
                    requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
                } else if (remoteGeneration < localGeneration) {
                    /* send all data with generation = localgeneration and version > 0 */
                    sendAll(gDigest, deltaEpStateMap, 0);
                } else if (remoteGeneration == localGeneration) {
                    /*
                        If the max remote version is greater then we request the remote node send us all the data
                        for this node with version greater than the max version number we have locally for this
                        node.
                        If the max remote version is lesser, then we send all the data we have locally for this node
                        with version greater than the max remote version.
                    */
                    if (maxRemoteVersion > maxLocalVersion) {
                        deltaGossipDigestList
                                .add(new GossipDigest(gDigest.getNode(), remoteGeneration, maxLocalVersion));
                    } else if (maxRemoteVersion < maxLocalVersion) {
                        /* send all data with generation = localgeneration and version > maxRemoteVersion */
                        sendAll(gDigest, deltaEpStateMap, maxRemoteVersion);
                    }
                }
            } else {
                /* We are here since we have no data for this node locally so request everything. */
                requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
            }
        }
    }

    /**
     *  Do a single 'shadow' round of gossip, where we do not modify any state
     *  Only used when replacing a node, to get and assume its states
     */
    public void doShadowRound() {
        buildSeedsList();
        // send a completely empty syn
        List<GossipDigest> gDigests = new ArrayList<>();
        GossipDigestSyn digestSynMessage = new GossipDigestSyn(ConfigDescriptor.getClusterName(), gDigests);
        MessageOut<GossipDigestSyn> message = new MessageOut<>(Verb.GOSSIP_DIGEST_SYN, digestSynMessage);
        inShadowRound = true;
        for (NetNode seed : seeds)
            MessagingService.instance().sendOneWay(message, seed);
        int slept = 0;
        try {
            while (true) {
                Thread.sleep(1000);
                if (!inShadowRound)
                    break;
                slept += 1000;
                if (slept > RING_DELAY)
                    throw new RuntimeException("Unable to gossip with any seeds");
            }
        } catch (InterruptedException wtf) {
            throw new RuntimeException(wtf);
        }
    }

    protected void finishShadowRound() {
        if (inShadowRound)
            inShadowRound = false;
    }

    protected boolean isInShadowRound() {
        return inShadowRound;
    }

    /**
     * Add an node we knew about previously, but whose state is unknown
     */
    public void addSavedNode(NetNode ep) {
        if (ep.equals(ConfigDescriptor.getLocalNode())) {
            logger.debug("Attempt to add self as saved node");
            return;
        }

        // preserve any previously known, in-memory data about the node (such as DC, RACK, and so on)
        NodeState epState = nodeStateMap.get(ep);
        if (epState != null) {
            logger.debug("not replacing a previous epState for {}, but reusing it: {}", ep, epState);
            epState.setHeartBeatState(new HeartBeatState(0));
        } else {
            epState = new NodeState(new HeartBeatState(0));
        }

        epState.markDead();
        nodeStateMap.put(ep, epState);
        unreachableNodes.put(ep, System.nanoTime());
        if (logger.isTraceEnabled())
            logger.trace("Adding saved node {} {}", ep, epState.getHeartBeatState().getGeneration());
    }

    public void addLocalApplicationState(ApplicationState state, VersionedValue value) {
        NetNode epAddr = ConfigDescriptor.getLocalNode();
        NodeState epState = nodeStateMap.get(epAddr);
        assert epState != null;
        // Fire "before change" notifications:
        doBeforeChangeNotifications(epAddr, epState, state, value);
        // Notifications may have taken some time, so preventively raise the version
        // of the new value, otherwise it could be ignored by the remote node
        // if another value with a newer version was received in the meantime:
        value = P2pServer.valueFactory.cloneWithHigherVersion(value);
        // Add to local application state and fire "on change" notifications:
        epState.addApplicationState(state, value);
        doOnChangeNotifications(epAddr, state, value);
    }

    public void addLocalApplicationStates(List<Pair<ApplicationState, VersionedValue>> states) {
        taskLock.lock();
        try {
            for (Pair<ApplicationState, VersionedValue> pair : states) {
                addLocalApplicationState(pair.left, pair.right);
            }
        } finally {
            taskLock.unlock();
        }
    }

    public void stop() {
        if (scheduledGossipTask != null)
            scheduledGossipTask.cancel(false);
        logger.info("Announcing shutdown");
        Uninterruptibles.sleepUninterruptibly(INTERVAL_IN_MILLIS * 2, TimeUnit.MILLISECONDS);
        MessageOut<?> message = new MessageOut<>(Verb.GOSSIP_SHUTDOWN);
        for (NetNode ep : liveNodes)
            MessagingService.instance().sendOneWay(message, ep);
    }

    public boolean isEnabled() {
        return true; // (scheduledGossipTask != null) && (!scheduledGossipTask.isCancelled());
    }

    public void addExpireTimeForNode(NetNode node, long expireTime) {
        if (logger.isDebugEnabled()) {
            logger.debug("adding expire time for node : {} ({})", node, expireTime);
        }
        expireTimeNodeMap.put(node, expireTime);
    }

    public static long computeExpireTime() {
        return System.currentTimeMillis() + Gossiper.A_VERY_LONG_TIME;
    }

    // @VisibleForTesting
    // public void injectApplicationState(NetNode node, ApplicationState state, VersionedValue value) {
    // NodeState localState = nodeStateMap.get(node);
    // localState.addApplicationState(state, value);
    // }
    // public boolean seenAnySeed() {
    // for (Map.Entry<NetNode, NodeState> entry : nodeStateMap.entrySet()) {
    // if (seeds.contains(entry.getKey()))
    // return true;
    // try {
    // if (entry.getValue().getApplicationStateMap().containsKey(ApplicationState.INTERNAL_IP)
    // && seeds.contains(NetNode
    // .getByName(entry.getValue().getApplicationState(ApplicationState.INTERNAL_IP).value)))
    // return true;
    // } catch (UnknownHostException e) {
    // throw new RuntimeException(e);
    // }
    // }
    // return false;
    // }
    //
    // public NetNode getFirstLiveSeedNode() {
    // for (NetNode seed : ConfigDescriptor.getSeedList()) {
    // if (FailureDetector.instance.isAlive(seed))
    // return seed;
    // }
    // throw new IllegalStateException("Unable to find any live seeds!");
    // }
    //
    // public NetNode getLiveSeedNode() {
    // INodeSnitch snitch = ConfigDescriptor.getNodeSnitch();
    // String dc = snitch.getDatacenter(ConfigDescriptor.getLocalNode());
    // for (NetNode seed : ConfigDescriptor.getSeedList()) {
    // if (FailureDetector.instance.isAlive(seed) && dc.equals(snitch.getDatacenter(seed)))
    // return seed;
    // }
    //
    // for (NetNode seed : ConfigDescriptor.getSeedList()) {
    // if (FailureDetector.instance.isAlive(seed))
    // return seed;
    // }
    // throw new IllegalStateException("Unable to find any live seeds!");
    // }
}

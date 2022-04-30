/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetNode;
import org.lealone.p2p.config.Config;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.util.BoundedStatsDeque;
import org.lealone.p2p.util.FSWriteError;
import org.lealone.p2p.util.FileUtils;
import org.lealone.p2p.util.P2pUtils;

/**
 * This FailureDetector is an implementation of the paper titled
 * "The Phi Accrual Failure Detector" by Hayashibara.
 * Check the paper and the <i>IFailureDetector</i> interface for details.
 */
public class FailureDetector implements IFailureDetector, FailureDetectorMBean {
    private static final Logger logger = LoggerFactory.getLogger(FailureDetector.class);
    private static final int SAMPLE_SIZE = 1000;
    protected static final long INITIAL_VALUE_NANOS = TimeUnit.NANOSECONDS.convert(getInitialValue(),
            TimeUnit.MILLISECONDS);

    public static final FailureDetector instance = new FailureDetector();

    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    private final double PHI_FACTOR = 1.0 / Math.log(10.0); // 0.434...

    private final Map<NetNode, ArrivalWindow> arrivalSamples = new Hashtable<>();
    private final List<IFailureDetectionEventListener> fdEvntListeners = new CopyOnWriteArrayList<>();

    public FailureDetector() {
        // Register this instance with JMX
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(P2pUtils.getJmxObjectName("FailureDetector")));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static long getInitialValue() {
        String newvalue = Config.getProperty("fd.initial.value.ms");
        if (newvalue == null) {
            return Gossiper.INTERVAL_IN_MILLIS * 2;
        } else {
            logger.info("Overriding FD INITIAL_VALUE to {}ms", newvalue);
            return Integer.parseInt(newvalue);
        }
    }

    @Override
    public String getAllNodeStates() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<NetNode, NodeState> entry : Gossiper.instance.nodeStateMap.entrySet()) {
            sb.append(entry.getKey()).append("\n");
            appendNodeState(sb, entry.getValue());
        }
        return sb.toString();
    }

    @Override
    public Map<String, String> getSimpleStates() {
        Map<String, String> nodesStatus = new HashMap<>(Gossiper.instance.nodeStateMap.size());
        for (Map.Entry<NetNode, NodeState> entry : Gossiper.instance.nodeStateMap.entrySet()) {
            if (entry.getValue().isAlive())
                nodesStatus.put(entry.getKey().toString(), "UP");
            else
                nodesStatus.put(entry.getKey().toString(), "DOWN");
        }
        return nodesStatus;
    }

    @Override
    public int getDownNodeCount() {
        int count = 0;
        for (Map.Entry<NetNode, NodeState> entry : Gossiper.instance.nodeStateMap.entrySet()) {
            if (!entry.getValue().isAlive())
                count++;
        }
        return count;
    }

    @Override
    public int getUpNodeCount() {
        int count = 0;
        for (Map.Entry<NetNode, NodeState> entry : Gossiper.instance.nodeStateMap.entrySet()) {
            if (entry.getValue().isAlive())
                count++;
        }
        return count;
    }

    @Override
    public String getNodeState(String address) throws UnknownHostException {
        StringBuilder sb = new StringBuilder();
        NodeState nodeState = Gossiper.instance.getNodeState(NetNode.getByName(address));
        appendNodeState(sb, nodeState);
        return sb.toString();
    }

    private void appendNodeState(StringBuilder sb, NodeState nodeState) {
        sb.append("  generation:").append(nodeState.getHeartBeatState().getGeneration()).append("\n");
        sb.append("  heartbeat:").append(nodeState.getHeartBeatState().getHeartBeatVersion()).append("\n");
        for (Map.Entry<ApplicationState, VersionedValue> state : nodeState.applicationState.entrySet()) {
            sb.append("  ").append(state.getKey()).append(":").append(state.getValue().value).append("\n");
        }
    }

    /**
     * Dump the inter arrival times for examination if necessary.
     */
    @Override
    public void dumpInterArrivalTimes() {
        File file = FileUtils.createTempFile("failuredetector-", ".dat");

        OutputStream os = null;
        try {
            os = new BufferedOutputStream(new FileOutputStream(file, true));
            os.write(toString().getBytes());
        } catch (IOException e) {
            throw new FSWriteError(e, file);
        } finally {
            FileUtils.closeQuietly(os);
        }
    }

    @Override
    public void setPhiConvictThreshold(double phi) {
        ConfigDescriptor.setPhiConvictThreshold(phi);
    }

    @Override
    public double getPhiConvictThreshold() {
        return ConfigDescriptor.getPhiConvictThreshold();
    }

    @Override
    public boolean isAlive(NetNode ep) {
        if (ep.equals(ConfigDescriptor.getLocalNode()))
            return true;

        NodeState epState = Gossiper.instance.getNodeState(ep);
        // we could assert not-null, but having isAlive fail screws a node over so badly that
        // it's worth being defensive here so minor bugs don't cause disproportionate
        // badness. (See lealone-1463 for an example).
        if (epState == null)
            logger.error("unknown node {}", ep);
        return epState != null && epState.isAlive();
    }

    @Override
    public void report(NetNode ep) {
        if (logger.isTraceEnabled())
            logger.trace("reporting {}", ep);
        long now = System.nanoTime();
        ArrivalWindow heartbeatWindow = arrivalSamples.get(ep);
        if (heartbeatWindow == null) {
            // avoid adding an empty ArrivalWindow to the Map
            heartbeatWindow = new ArrivalWindow(SAMPLE_SIZE);
            heartbeatWindow.add(now);
            arrivalSamples.put(ep, heartbeatWindow);
        } else {
            heartbeatWindow.add(now);
        }
    }

    @Override
    public void interpret(NetNode ep) {
        ArrivalWindow hbWnd = arrivalSamples.get(ep);
        if (hbWnd == null) {
            return;
        }
        long now = System.nanoTime();
        double phi = hbWnd.phi(now);
        if (logger.isTraceEnabled())
            logger.trace("PHI for {} : {}", ep, phi);

        if (PHI_FACTOR * phi > getPhiConvictThreshold()) {
            logger.trace("notifying listeners that {} is down", ep);
            logger.trace("intervals: {} mean: {}", hbWnd, hbWnd.mean());
            for (IFailureDetectionEventListener listener : fdEvntListeners) {
                listener.convict(ep, phi);
            }
        }
    }

    @Override
    public void forceConviction(NetNode ep) {
        logger.debug("Forcing conviction of {}", ep);
        for (IFailureDetectionEventListener listener : fdEvntListeners) {
            listener.convict(ep, getPhiConvictThreshold());
        }
    }

    @Override
    public void remove(NetNode ep) {
        arrivalSamples.remove(ep);
    }

    @Override
    public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) {
        fdEvntListeners.add(listener);
    }

    @Override
    public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) {
        fdEvntListeners.remove(listener);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Set<NetNode> eps = arrivalSamples.keySet();

        sb.append("-----------------------------------------------------------------------");
        for (NetNode ep : eps) {
            ArrivalWindow hWnd = arrivalSamples.get(ep);
            sb.append(ep + " : ");
            sb.append(hWnd);
            sb.append(System.getProperty("line.separator"));
        }
        sb.append("-----------------------------------------------------------------------");
        return sb.toString();
    }

    private static class ArrivalWindow {
        private static final Logger logger = LoggerFactory.getLogger(ArrivalWindow.class);
        private long tLast = 0L;
        private final BoundedStatsDeque arrivalIntervals;

        // this is useless except to provide backwards compatibility in phi_convict_threshold,
        // because everyone seems pretty accustomed to the default of 8, and users who have
        // already tuned their phi_convict_threshold for their own environments won't need to
        // change.
        // private final double PHI_FACTOR = 1.0 / Math.log(10.0);

        // in the event of a long partition, never record an interval longer than the rpc timeout,
        // since if a host is regularly experiencing connectivity problems lasting this long we'd
        // rather mark it down quickly instead of adapting
        // this value defaults to the same initial value the FD is seeded with
        private final long MAX_INTERVAL_IN_NANO = getMaxInterval();

        ArrivalWindow(int size) {
            arrivalIntervals = new BoundedStatsDeque(size);
        }

        private static long getMaxInterval() {
            String newvalue = Config.getProperty("fd.max.interval.ms");
            if (newvalue == null) {
                return FailureDetector.INITIAL_VALUE_NANOS;
            } else {
                logger.info("Overriding FD MAX_INTERVAL to {}ms", newvalue);
                return TimeUnit.NANOSECONDS.convert(Integer.parseInt(newvalue), TimeUnit.MILLISECONDS);
            }
        }

        synchronized void add(long value) {
            assert tLast >= 0;
            if (tLast > 0L) {
                long interArrivalTime = (value - tLast);
                if (interArrivalTime <= MAX_INTERVAL_IN_NANO)
                    arrivalIntervals.add(interArrivalTime);
                else
                    logger.debug("Ignoring interval time of {}", interArrivalTime);
            } else {
                // We use a very large initial interval since the "right" average depends on the cluster size
                // and it's better to err high (false negatives, which will be corrected by waiting a bit longer)
                // than low (false positives, which cause "flapping").
                arrivalIntervals.add(FailureDetector.INITIAL_VALUE_NANOS);
            }
            tLast = value;
        }

        double mean() {
            return arrivalIntervals.mean();
        }

        // see lealone-2597 for an explanation of the math at work here.
        double phi(long tnow) {
            assert arrivalIntervals.size() > 0 && tLast > 0; // should not be called before any samples arrive
            long t = tnow - tLast;
            return t / mean();
        }

        @Override
        public String toString() {
            return P2pUtils.join(arrivalIntervals.iterator(), " ");
        }
    }
}

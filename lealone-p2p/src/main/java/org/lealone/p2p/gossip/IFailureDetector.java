/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip;

import org.lealone.net.NetNode;

/**
 * An interface that provides an application with the ability
 * to query liveness information of a node in the cluster. It
 * also exposes methods which help an application register callbacks
 * for notifications of liveness information of nodes.
 */
public interface IFailureDetector {
    /**
     * Failure Detector's knowledge of whether a node is up or
     * down.
     *
     * @param ep node in question.
     * @return true if UP and false if DOWN.
     */
    public boolean isAlive(NetNode ep);

    /**
     * This method is invoked by any entity wanting to interrogate the status of an node.
     * In our case it would be the Gossiper. The Failure Detector will then calculate Phi and
     * deem an node as suspicious or alive as explained in the Hayashibara paper.
     * <p/>
     * param ep node for which we interpret the inter arrival times.
     */
    public void interpret(NetNode ep);

    /**
     * This method is invoked by the receiver of the heartbeat. In our case it would be
     * the Gossiper. Gossiper inform the Failure Detector on receipt of a heartbeat. The
     * FailureDetector will then sample the arrival time as explained in the paper.
     * <p/>
     * param ep node being reported.
     */
    public void report(NetNode ep);

    /**
     * remove node from failure detector
     */
    public void remove(NetNode ep);

    /**
     * force conviction of node in the failure detector
     */
    public void forceConviction(NetNode ep);

    /**
     * Register interest for Failure Detector events.
     *
     * @param listener implementation of an application provided IFailureDetectionEventListener
     */
    public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener);

    /**
     * Un-register interest for Failure Detector events.
     *
     * @param listener implementation of an application provided IFailureDetectionEventListener
     */
    public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener);
}

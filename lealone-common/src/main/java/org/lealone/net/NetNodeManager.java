/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.lealone.db.IDatabase;

public interface NetNodeManager {

    Set<NetNode> getLiveNodes();

    String[] assignNodes(IDatabase db);

    default int getRpcTimeout() {
        return 0;
    }

    default NetNode getNode(String hostId) {
        return null;
    }

    default Set<NetNode> getNodes(List<String> hostIds) {
        return null;
    }

    default String getHostId(NetNode node) {
        return null;
    }

    default List<NetNode> getReplicationNodes(IDatabase db, Set<NetNode> oldReplicationNodes,
            Set<NetNode> candidateNodes) {
        return null;
    }

    default Collection<String> getRecognizedReplicationStrategyOptions(String strategyName) {
        return Collections.<String> singleton("replication_factor");
    }

    default Collection<String> getRecognizedNodeAssignmentStrategyOptions(String strategyName) {
        return Collections.<String> singleton("assignment_factor");
    }

    default String getDefaultReplicationStrategy() {
        return null;
    }

    default int getDefaultReplicationFactor() {
        return 1;
    }

    default String getDefaultNodeAssignmentStrategy() {
        return null;
    }

    default int getDefaultNodeAssignmentFactor() {
        return 1;
    }

    default boolean isLocal() {
        return false;
    }
}

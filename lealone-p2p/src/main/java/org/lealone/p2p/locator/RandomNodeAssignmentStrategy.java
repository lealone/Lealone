/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.net.NetNode;

public class RandomNodeAssignmentStrategy extends AbstractNodeAssignmentStrategy {

    private static final Random random = new Random();

    public RandomNodeAssignmentStrategy(String dbName, INodeSnitch snitch, Map<String, String> configOptions) {
        super(dbName, snitch, configOptions);
    }

    @Override
    public int getAssignmentFactor() {
        return Integer.parseInt(configOptions.get("assignment_factor"));
    }

    @Override
    public void validateOptions() throws ConfigException {
        String af = configOptions.get("assignment_factor");
        if (af == null)
            throw new ConfigException(
                    this.getClass().getSimpleName() + " requires a assignment_factor strategy option.");
        validateAssignmentFactor(af);
    }

    @Override
    public Collection<String> recognizedOptions() {
        return Collections.<String> singleton("assignment_factor");
    }

    @Override
    public List<NetNode> assignNodes(TopologyMetaData metaData, Set<NetNode> oldNodes, Set<NetNode> candidateNodes,
            boolean includeOldNodes) {
        HashSet<NetNode> all = new HashSet<>(candidateNodes);
        all.removeAll(oldNodes);
        int total = all.size();
        int need = getAssignmentFactor();
        if (includeOldNodes)
            need -= oldNodes.size();

        ArrayList<NetNode> nodes = new ArrayList<>(need);

        if (total > 0) {
            ArrayList<NetNode> list = new ArrayList<>(all);
            Set<Integer> indexSet = new HashSet<>(need);
            if (need >= total) {
                need = total;
                for (int i = 0; i < total; i++) {
                    indexSet.add(i);
                }
            } else {
                while (true) {
                    int i = random.nextInt(total);
                    if (!oldNodes.contains(list.get(i))) {
                        indexSet.add(i);
                        if (indexSet.size() == need)
                            break;
                    }
                }
            }
            for (int i : indexSet) {
                nodes.add(list.get(i));
            }
        }

        // 不够时，从原来的节点中取
        if (nodes.size() < need) {
            getFromOldNodes(oldNodes, nodes, need);
        }
        return nodes;
    }
}

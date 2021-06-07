/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.net.NetNode;
import org.lealone.p2p.gossip.Gossiper;

public class LoadBasedNodeAssignmentStrategy extends AbstractNodeAssignmentStrategy {

    public LoadBasedNodeAssignmentStrategy(String dbName, INodeSnitch snitch, Map<String, String> configOptions) {
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

        class LoadInfo implements Comparable<LoadInfo> {
            String load;
            int index;

            public LoadInfo(String load, int index) {
                super();
                this.load = load;
                this.index = index;
            }

            @Override
            public int compareTo(LoadInfo o) {
                return this.load.compareTo(o.load);
            }
        }

        ArrayList<NetNode> allList = new ArrayList<>(all);
        LoadInfo[] loadInfoList = new LoadInfo[total];
        for (int i = 0; i < total; i++) {
            String load = Gossiper.instance.getLoad(allList.get(i));
            loadInfoList[i] = new LoadInfo(load, i);
        }

        // 按load从小到大排序，优先分配load小的节点
        Arrays.sort(loadInfoList);

        ArrayList<NetNode> nodes = new ArrayList<>(need);

        if (total > 0) {
            int count = 0;
            for (LoadInfo loadInfo : loadInfoList) {
                NetNode e = allList.get(loadInfo.index);
                if (!oldNodes.contains(e)) {
                    nodes.add(e);
                    if (++count >= need)
                        break;
                }
            }
        }

        // 不够时，从原来的节点中取
        if (nodes.size() < need) {
            getFromOldNodes(oldNodes, nodes, need);
        }
        return nodes;
    }
}

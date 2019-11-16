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
package org.lealone.p2p.locator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.util.StringUtils;
import org.lealone.net.NetNode;

public class ManualNodeAssignmentStrategy extends AbstractNodeAssignmentStrategy {

    public ManualNodeAssignmentStrategy(String dbName, INodeSnitch snitch, Map<String, String> configOptions) {
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
        ArrayList<String> options = new ArrayList<>(2);
        options.add("assignment_factor");
        options.add("host_id_list");
        return options;
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
            int count = 0;
            String[] hostIds = StringUtils.arraySplit(configOptions.get("host_id_list"), ',');
            for (String hostId : hostIds) {
                NetNode e = metaData.getNode(hostId);
                if (e == null) {
                    throw new ConfigException("HostId " + hostId + " not found.");
                }
                if (candidateNodes.contains(e)) {
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

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

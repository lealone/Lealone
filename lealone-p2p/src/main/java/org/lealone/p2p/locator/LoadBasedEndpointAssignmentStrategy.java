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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.net.NetEndpoint;
import org.lealone.p2p.gms.Gossiper;

public class LoadBasedEndpointAssignmentStrategy extends AbstractEndpointAssignmentStrategy {

    public LoadBasedEndpointAssignmentStrategy(String dbName, IEndpointSnitch snitch,
            Map<String, String> configOptions) {
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
    public List<NetEndpoint> assignEndpoints(TopologyMetaData metaData, Set<NetEndpoint> oldEndpoints,
            Set<NetEndpoint> candidateEndpoints, boolean includeOldEndpoints) {
        HashSet<NetEndpoint> all = new HashSet<>(candidateEndpoints);
        all.removeAll(oldEndpoints);
        int total = all.size();
        int need = getAssignmentFactor();
        if (includeOldEndpoints)
            need -= oldEndpoints.size();

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

        ArrayList<NetEndpoint> allList = new ArrayList<>(all);
        LoadInfo[] loadInfoList = new LoadInfo[total];
        for (int i = 0; i < total; i++) {
            String load = Gossiper.instance.getLoad(allList.get(i));
            loadInfoList[i] = new LoadInfo(load, i);
        }

        // 按load从小到大排序，优先分配load小的节点
        Arrays.sort(loadInfoList);

        ArrayList<NetEndpoint> endpoints = new ArrayList<>(need);

        if (total > 0) {
            int count = 0;
            for (LoadInfo loadInfo : loadInfoList) {
                NetEndpoint e = allList.get(loadInfo.index);
                if (!oldEndpoints.contains(e)) {
                    endpoints.add(e);
                    if (++count >= need)
                        break;
                }
            }
        }

        // 不够时，从原来的节点中取
        if (endpoints.size() < need) {
            getFromOldEndpoints(oldEndpoints, endpoints, need);
        }
        return endpoints;
    }
}

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.net.NetEndpoint;

/**
 * This class returns the nodes responsible for a given
 * key but does not respect rack awareness. Basically
 * returns the RF nodes that lie right next to each other
 * on the ring.
 */
public class SimpleStrategy extends AbstractReplicationStrategy {
    public SimpleStrategy(String dbName, TopologyMetaData metaData, IEndpointSnitch snitch,
            Map<String, String> configOptions) {
        super(dbName, metaData, snitch, configOptions);
    }

    @Override
    public List<NetEndpoint> calculateReplicationEndpoints(TopologyMetaData metaData,
            Set<NetEndpoint> oldReplicationEndpoints, Set<NetEndpoint> candidateEndpoints,
            boolean includeOldReplicationEndpoints) {
        int replicas = getReplicationFactor();
        if (includeOldReplicationEndpoints)
            replicas -= oldReplicationEndpoints.size();
        ArrayList<String> hostIds = metaData.getSortedHostIds();
        List<NetEndpoint> endpoints = new ArrayList<NetEndpoint>(replicas);

        if (hostIds.isEmpty())
            return endpoints;

        Iterator<String> iter = hostIds.iterator();
        while (endpoints.size() < replicas && iter.hasNext()) {
            NetEndpoint ep = metaData.getEndpoint(iter.next());
            if (candidateEndpoints.contains(ep) && !oldReplicationEndpoints.contains(ep) && !endpoints.contains(ep))
                endpoints.add(ep);
        }

        // 不够时，从原来的复制节点中取
        if (endpoints.size() < replicas) {
            Iterator<NetEndpoint> old = oldReplicationEndpoints.iterator();
            while (endpoints.size() < replicas && old.hasNext()) {
                NetEndpoint ep = old.next();
                if (!endpoints.contains(ep))
                    endpoints.add(ep);
            }
        }
        return endpoints;
    }

    @Override
    public int getReplicationFactor() {
        return Integer.parseInt(this.configOptions.get("replication_factor"));
    }

    @Override
    public void validateOptions() throws ConfigException {
        String rf = configOptions.get("replication_factor");
        if (rf == null)
            throw new ConfigException("SimpleStrategy requires a replication_factor strategy option.");
        validateReplicationFactor(rf);
    }

    @Override
    public Collection<String> recognizedOptions() {
        return Collections.<String> singleton("replication_factor");
    }
}

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
import java.util.Comparator;
import java.util.List;

import org.lealone.net.NetEndpoint;
import org.lealone.p2p.config.ConfigDescriptor;

public abstract class AbstractEndpointSnitch implements IEndpointSnitch {
    @Override
    public abstract int compareEndpoints(NetEndpoint target, NetEndpoint a1, NetEndpoint a2);

    /**
     * Sorts the <tt>Collection</tt> of node addresses by proximity to the given address
     * @param address the address to sort by proximity to
     * @param unsortedAddress the nodes to sort 
     * @return a new sorted <tt>List</tt>
     */
    @Override
    public List<NetEndpoint> getSortedListByProximity(NetEndpoint address, Collection<NetEndpoint> unsortedAddress) {
        List<NetEndpoint> preferred = new ArrayList<NetEndpoint>(unsortedAddress);
        sortByProximity(address, preferred);
        return preferred;
    }

    /**
     * Sorts the <tt>List</tt> of node addresses, in-place, by proximity to the given address
     * @param address the address to sort the proximity by
     * @param addresses the nodes to sort
     */
    @Override
    public void sortByProximity(final NetEndpoint address, List<NetEndpoint> addresses) {
        Collections.sort(addresses, new Comparator<NetEndpoint>() {
            @Override
            public int compare(NetEndpoint a1, NetEndpoint a2) {
                return compareEndpoints(address, a1, a2);
            }
        });
    }

    @Override
    public void gossiperStarting() {
        // noop by default
    }

    @Override
    public boolean isWorthMergingForRangeQuery(List<NetEndpoint> merged, List<NetEndpoint> l1, List<NetEndpoint> l2) {
        // Querying remote DC is likely to be an order of magnitude slower than
        // querying locally, so 2 queries to local nodes is likely to still be
        // faster than 1 query involving remote ones
        boolean mergedHasRemote = hasRemoteNode(merged);
        return mergedHasRemote ? hasRemoteNode(l1) || hasRemoteNode(l2) : true;
    }

    private boolean hasRemoteNode(List<NetEndpoint> l) {
        String localDc = ConfigDescriptor.getLocalDataCenter();
        for (NetEndpoint ep : l) {
            if (!localDc.equals(getDatacenter(ep)))
                return true;
        }
        return false;
    }
}

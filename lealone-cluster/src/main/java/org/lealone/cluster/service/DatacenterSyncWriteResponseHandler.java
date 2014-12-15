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
package org.lealone.cluster.service;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.db.ConsistencyLevel;
import org.lealone.cluster.db.Keyspace;
import org.lealone.cluster.db.WriteType;
import org.lealone.cluster.locator.IEndpointSnitch;
import org.lealone.cluster.locator.NetworkTopologyStrategy;
import org.lealone.cluster.net.MessageIn;

/**
 * This class blocks for a quorum of responses _in all datacenters_ (CL.EACH_QUORUM).
 */
public class DatacenterSyncWriteResponseHandler extends AbstractWriteResponseHandler {
    private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

    private final NetworkTopologyStrategy strategy;
    private final HashMap<String, AtomicInteger> responses = new HashMap<String, AtomicInteger>();
    private final AtomicInteger acks = new AtomicInteger(0);

    public DatacenterSyncWriteResponseHandler(Collection<InetAddress> naturalEndpoints, Collection<InetAddress> pendingEndpoints,
            ConsistencyLevel consistencyLevel, Keyspace keyspace, Runnable callback, WriteType writeType) {
        // Response is been managed by the map so make it 1 for the superclass.
        super(keyspace, naturalEndpoints, pendingEndpoints, consistencyLevel, callback, writeType);
        assert consistencyLevel == ConsistencyLevel.EACH_QUORUM;

        strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();

        for (String dc : strategy.getDatacenters()) {
            int rf = strategy.getReplicationFactor(dc);
            responses.put(dc, new AtomicInteger((rf / 2) + 1));
        }

        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        for (InetAddress pending : pendingEndpoints) {
            responses.get(snitch.getDatacenter(pending)).incrementAndGet();
        }
    }

    public void response(MessageIn message) {
        String dataCenter = message == null ? DatabaseDescriptor.getLocalDataCenter() : snitch.getDatacenter(message.from);

        responses.get(dataCenter).getAndDecrement();
        acks.incrementAndGet();

        for (AtomicInteger i : responses.values()) {
            if (i.get() > 0)
                return;
        }

        // all the quorum conditions are met
        signal();
    }

    protected int ackCount() {
        return acks.get();
    }

    public boolean isLatencyForSnitch() {
        return false;
    }
}

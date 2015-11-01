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
package org.lealone.cluster.streaming;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.lealone.cluster.dht.Range;
import org.lealone.cluster.dht.Token;
import org.lealone.cluster.utils.UUIDGen;

/**
 * {@link StreamPlan} is a helper class that builds StreamOperation of given configuration.
 *
 * This is the class you want to use for building streaming plan and starting streaming.
 */
public class StreamPlan {

    private final UUID planId = UUIDGen.getTimeUUID();
    private final String description;
    private final List<StreamEventHandler> handlers = new ArrayList<>();
    private final StreamCoordinator coordinator;

    private boolean flushBeforeTransfer = true;

    /**
     * Start building stream plan.
     *
     * @param description Stream type that describes this StreamPlan
     */
    public StreamPlan(String description) {
        this(description, 1);
    }

    public StreamPlan(String description, int connectionsPerHost) {
        this.description = description;
        this.coordinator = new StreamCoordinator(connectionsPerHost, new DefaultConnectionFactory());
    }

    /**
     * Request data in {@code dbName} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param connecting Actual connecting address for the endpoint
     * @param dbName name of database
     * @param ranges ranges to fetch
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddress from, InetAddress connecting, String dbName,
            Collection<Range<Token>> ranges) {
        return requestRanges(from, connecting, dbName, ranges, new String[0]);
    }

    /**
     * Request data in {@code tableNames} under {@code dbName} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param connecting Actual connecting address for the endpoint
     * @param dbName name of database
     * @param ranges ranges to fetch
     * @param tableNames specific table names
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddress from, InetAddress connecting, String dbName,
            Collection<Range<Token>> ranges, String... tableNames) {
        StreamSession session = coordinator.getOrCreateNextSession(from, connecting);
        session.addStreamRequest(dbName, ranges, Arrays.asList(tableNames));
        return this;
    }

    /**
     * Add transfer task to send data of specific {@code tableNames} under {@code dbName} and {@code ranges}.
     *
     * @see #transferRanges(java.net.InetAddress, java.net.InetAddress, String, java.util.Collection, String...)
     */
    public StreamPlan transferRanges(InetAddress to, String dbName, Collection<Range<Token>> ranges,
            String... tableNames) {
        return transferRanges(to, to, dbName, ranges, tableNames);
    }

    /**
     * Add transfer task to send data of specific database and ranges.
     *
     * @param to endpoint address of receiver
     * @param connecting Actual connecting address of the endpoint
     * @param dbName name of database
     * @param ranges ranges to send
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddress to, InetAddress connecting, String dbName,
            Collection<Range<Token>> ranges) {
        return transferRanges(to, connecting, dbName, ranges, new String[0]);
    }

    /**
     * Add transfer task to send data of specific {@code tableNames} under {@code dbName} and {@code ranges}.
     *
     * @param to endpoint address of receiver
     * @param connecting Actual connecting address of the endpoint
     * @param dbName name of database
     * @param ranges ranges to send
     * @param tableNames specific tables
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddress to, InetAddress connecting, String dbName,
            Collection<Range<Token>> ranges, String... tableNames) {
        StreamSession session = coordinator.getOrCreateNextSession(to, connecting);
        session.addTransferRanges(dbName, ranges, Arrays.asList(tableNames), flushBeforeTransfer);
        return this;
    }

    public StreamPlan listeners(StreamEventHandler handler, StreamEventHandler... handlers) {
        this.handlers.add(handler);
        if (handlers != null)
            Collections.addAll(this.handlers, handlers);
        return this;
    }

    /**
     * Set custom StreamConnectionFactory to be used for establishing connection
     *
     * @param factory StreamConnectionFactory to use
     * @return self
     */
    public StreamPlan connectionFactory(StreamConnectionFactory factory) {
        this.coordinator.setConnectionFactory(factory);
        return this;
    }

    /**
     * @return true if this plan has no plan to execute
     */
    public boolean isEmpty() {
        return !coordinator.hasActiveSessions();
    }

    /**
     * Execute this {@link StreamPlan} asynchronously.
     *
     * @return Future {@link StreamState} that you can use to listen on progress of streaming.
     */
    public StreamResultFuture execute() {
        return StreamResultFuture.init(planId, description, handlers, coordinator);
    }

    /**
     * Set flushBeforeTransfer option.
     * When it's true, will flush before streaming ranges. (Default: true)
     *
     * @param flushBeforeTransfer set to true when the node should flush before transfer
     * @return this object for chaining
     */
    public StreamPlan flushBeforeTransfer(boolean flushBeforeTransfer) {
        this.flushBeforeTransfer = flushBeforeTransfer;
        return this;
    }
}

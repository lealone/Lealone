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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.lealone.cluster.db.ConsistencyLevel;
import org.lealone.cluster.db.Keyspace;
import org.lealone.cluster.db.WriteType;
import org.lealone.cluster.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles blocking writes for ONE, ANY, TWO, THREE, QUORUM, and ALL consistency levels.
 */
public class WriteResponseHandler extends AbstractWriteResponseHandler {
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);

    protected volatile int responses;
    private static final AtomicIntegerFieldUpdater<WriteResponseHandler> responsesUpdater = AtomicIntegerFieldUpdater
            .newUpdater(WriteResponseHandler.class, "responses");

    public WriteResponseHandler(Collection<InetAddress> writeEndpoints, Collection<InetAddress> pendingEndpoints,
            ConsistencyLevel consistencyLevel, Keyspace keyspace, Runnable callback, WriteType writeType) {
        super(keyspace, writeEndpoints, pendingEndpoints, consistencyLevel, callback, writeType);
        responses = totalBlockFor();
    }

    public WriteResponseHandler(InetAddress endpoint, WriteType writeType, Runnable callback) {
        this(Arrays.asList(endpoint), Collections.<InetAddress> emptyList(), ConsistencyLevel.ONE, null, callback,
                writeType);
    }

    public WriteResponseHandler(InetAddress endpoint, WriteType writeType) {
        this(endpoint, writeType, null);
    }

    @Override
    public void response(MessageIn<Object> m) {
        if (responsesUpdater.decrementAndGet(this) == 0)
            signal();
    }

    @Override
    protected int ackCount() {
        return totalBlockFor() - responses;
    }

    @Override
    public boolean isLatencyForSnitch() {
        return false;
    }
}

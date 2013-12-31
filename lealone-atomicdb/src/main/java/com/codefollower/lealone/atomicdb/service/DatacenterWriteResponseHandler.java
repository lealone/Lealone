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
package com.codefollower.lealone.atomicdb.service;

import java.net.InetAddress;
import java.util.Collection;

import com.codefollower.lealone.atomicdb.config.DatabaseDescriptor;
import com.codefollower.lealone.atomicdb.db.ConsistencyLevel;
import com.codefollower.lealone.atomicdb.db.Keyspace;
import com.codefollower.lealone.atomicdb.db.WriteType;
import com.codefollower.lealone.atomicdb.locator.IEndpointSnitch;
import com.codefollower.lealone.atomicdb.net.MessageIn;

/**
 * This class blocks for a quorum of responses _in the local datacenter only_ (CL.LOCAL_QUORUM).
 */
public class DatacenterWriteResponseHandler extends WriteResponseHandler
{
    private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

    public DatacenterWriteResponseHandler(Collection<InetAddress> naturalEndpoints,
                                          Collection<InetAddress> pendingEndpoints,
                                          ConsistencyLevel consistencyLevel,
                                          Keyspace keyspace,
                                          Runnable callback,
                                          WriteType writeType)
    {
        super(naturalEndpoints, pendingEndpoints, consistencyLevel, keyspace, callback, writeType);
        assert consistencyLevel.isDatacenterLocal();
    }

    @Override
    public void response(MessageIn message)
    {
        if (message == null || DatabaseDescriptor.getLocalDataCenter().equals(snitch.getDatacenter(message.from)))
        {
            if (responses.decrementAndGet() == 0)
                signal();
        }
    }
}

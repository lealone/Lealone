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
package org.lealone.aose.locator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.common.exceptions.ConfigurationException;
import org.lealone.net.NetEndpoint;

public class LocalStrategy extends AbstractReplicationStrategy {
    public LocalStrategy(String dbName, TopologyMetaData metaData, IEndpointSnitch snitch,
            Map<String, String> configOptions) {
        super(dbName, metaData, snitch, configOptions);
    }

    /**
     * We need to override this even if we override calculateReplicationEndpoints,
     * because the default implementation depends on token calculations but
     * LocalStrategy may be used before tokens are set up.
     */
    @Override
    public ArrayList<NetEndpoint> getReplicationEndpoints(String hostId, Set<NetEndpoint> candidateEndpoints) {
        ArrayList<NetEndpoint> l = new ArrayList<NetEndpoint>(1);
        l.add(ConfigDescriptor.getLocalEndpoint());
        return l;
    }

    @Override
    public List<NetEndpoint> calculateReplicationEndpoints(String searchHostId, TopologyMetaData metadata,
            Set<NetEndpoint> candidateEndpoints) {
        return Collections.singletonList(ConfigDescriptor.getLocalEndpoint());
    }

    @Override
    public int getReplicationFactor() {
        return 1;
    }

    @Override
    public void validateOptions() throws ConfigurationException {
    }

    @Override
    public Collection<String> recognizedOptions() {
        // LocalStrategy doesn't expect any options.
        return Collections.<String> emptySet();
    }
}

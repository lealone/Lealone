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

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.aose.util.Utils;
import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetEndpoint;

/**
 * A abstract parent for all replication strategies.
*/
public abstract class AbstractReplicationStrategy {
    private static final Logger logger = LoggerFactory.getLogger(AbstractReplicationStrategy.class);

    protected final Map<String, String> configOptions;
    private final TopologyMetaData metaData;
    private final String dbName;

    AbstractReplicationStrategy(String dbName, TopologyMetaData metaData, IEndpointSnitch snitch,
            Map<String, String> configOptions) {
        assert dbName != null;
        assert snitch != null;
        assert metaData != null;
        this.metaData = metaData;
        this.configOptions = configOptions == null ? Collections.<String, String> emptyMap() : configOptions;
        this.dbName = dbName;
        // lazy-initialize keyspace itself since we don't create them until after the replication strategies
    }

    /**
     * calculate the natural endpoints for the given token
     *
     * @see #getReplicationEndpoints(org.lealone.daose.dht.RingPosition)
     *
     * @param searchHostId the token the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public abstract List<NetEndpoint> calculateReplicationEndpoints(TopologyMetaData metaData,
            Set<NetEndpoint> oldReplicationEndpoints, Set<NetEndpoint> candidateEndpoints);

    /**
     * calculate the RF based on strategy_options. When overwriting, ensure that this get()
     *  is FAST, as this is called often.
     *
     * @return the replication factor
     */
    public abstract int getReplicationFactor();

    public abstract void validateOptions() throws ConfigException;

    public List<NetEndpoint> getReplicationEndpoints(Set<NetEndpoint> oldReplicationEndpoints,
            Set<NetEndpoint> candidateEndpoints) {
        TopologyMetaData tm = metaData.cachedOnlyTokenMap();
        return calculateReplicationEndpoints(tm, oldReplicationEndpoints, candidateEndpoints);
    }

    /*
     * The options recognized by the strategy.
     * The empty collection means that no options are accepted, but null means
     * that any option is accepted.
     */
    public Collection<String> recognizedOptions() {
        // We default to null for backward compatibility sake
        return null;
    }

    protected void validateReplicationFactor(String rf) throws ConfigException {
        try {
            if (Integer.parseInt(rf) < 0) {
                throw new ConfigException("Replication factor must be non-negative; found " + rf);
            }
        } catch (NumberFormatException e2) {
            throw new ConfigException("Replication factor must be numeric; found " + rf);
        }
    }

    private void validateExpectedOptions() throws ConfigException {
        Collection<?> expectedOptions = recognizedOptions();
        if (expectedOptions == null)
            return;

        for (String key : configOptions.keySet()) {
            if (!expectedOptions.contains(key))
                throw new ConfigException(
                        String.format("Unrecognized strategy option {%s} passed to %s for keyspace %s", key,
                                getClass().getSimpleName(), dbName));
        }
    }

    private static AbstractReplicationStrategy createInternal(String dbName,
            Class<? extends AbstractReplicationStrategy> strategyClass, TopologyMetaData metaData,
            IEndpointSnitch snitch, Map<String, String> strategyOptions) throws ConfigException {
        AbstractReplicationStrategy strategy;
        Class<?>[] parameterTypes = new Class[] { String.class, TopologyMetaData.class, IEndpointSnitch.class,
                Map.class };
        try {
            Constructor<? extends AbstractReplicationStrategy> constructor = strategyClass
                    .getConstructor(parameterTypes);
            strategy = constructor.newInstance(dbName, metaData, snitch, strategyOptions);
        } catch (Exception e) {
            throw new ConfigException("Error constructing replication strategy class", e);
        }
        return strategy;
    }

    public static AbstractReplicationStrategy createReplicationStrategy(String dbName,
            Class<? extends AbstractReplicationStrategy> strategyClass, TopologyMetaData metaData,
            IEndpointSnitch snitch, Map<String, String> strategyOptions) {
        try {
            AbstractReplicationStrategy strategy = createInternal(dbName, strategyClass, metaData, snitch,
                    strategyOptions);

            // Because we used to not properly validate unrecognized options, we only log a warning if we find one.
            try {
                strategy.validateExpectedOptions();
            } catch (ConfigException e) {
                logger.warn("Ignoring {}", e.getMessage());
            }

            strategy.validateOptions();
            return strategy;
        } catch (ConfigException e) {
            // If that happens at this point, there is nothing we can do about it.
            throw new RuntimeException(e);
        }
    }

    public static void validateReplicationStrategy(String dbName,
            Class<? extends AbstractReplicationStrategy> strategyClass, TopologyMetaData metaData,
            IEndpointSnitch snitch, Map<String, String> strategyOptions) throws ConfigException {
        AbstractReplicationStrategy strategy = createInternal(dbName, strategyClass, metaData, snitch, strategyOptions);
        strategy.validateExpectedOptions();
        strategy.validateOptions();
    }

    public static Class<AbstractReplicationStrategy> getClass(String cls) throws ConfigException {
        String className = cls.contains(".") ? cls
                : AbstractReplicationStrategy.class.getPackage().getName() + "." + cls;
        Class<AbstractReplicationStrategy> strategyClass = Utils.classForName(className, "replication strategy");
        if (!AbstractReplicationStrategy.class.isAssignableFrom(strategyClass)) {
            throw new ConfigException(String.format(
                    "Specified replication strategy class (%s) is not derived from AbstractReplicationStrategy",
                    className));
        }
        return strategyClass;
    }
}

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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.lealone.aose.util.Utils;
import org.lealone.common.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A abstract parent for all replication strategies.
*/
public abstract class AbstractReplicationStrategy {
    private static final Logger logger = LoggerFactory.getLogger(AbstractReplicationStrategy.class);

    protected final Map<String, String> configOptions;
    private final TopologyMetaData metaData;
    private final Map<Integer, ArrayList<InetAddress>> cachedEndpoints = new NonBlockingHashMap<>();
    private final String dbName;

    // track when the token range changes, signaling we need to invalidate our endpoint cache
    private volatile long lastInvalidatedVersion = 0;

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
    public abstract List<InetAddress> calculateReplicationEndpoints(Integer searchHostId, TopologyMetaData metaData);

    /**
     * calculate the RF based on strategy_options. When overwriting, ensure that this get()
     *  is FAST, as this is called often.
     *
     * @return the replication factor
     */
    public abstract int getReplicationFactor();

    public abstract void validateOptions() throws ConfigurationException;

    public ArrayList<InetAddress> getCachedEndpoints(Integer hostId) {
        long lastVersion = metaData.getRingVersion();

        if (lastVersion > lastInvalidatedVersion) {
            synchronized (this) {
                if (lastVersion > lastInvalidatedVersion) {
                    if (logger.isDebugEnabled())
                        logger.debug("clearing cached endpoints");
                    cachedEndpoints.clear();
                    lastInvalidatedVersion = lastVersion;
                }
            }
        }

        return cachedEndpoints.get(hostId);
    }

    /**
     * get the (possibly cached) endpoints that should store the given Token.
     * Note that while the endpoints are conceptually a Set (no duplicates will be included),
     * we return a List to avoid an extra allocation when sorting by proximity later
     * @param searchPosition the position the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public ArrayList<InetAddress> getReplicationEndpoints(Integer hostId) {
        ArrayList<InetAddress> endpoints = getCachedEndpoints(hostId);
        if (endpoints == null) {
            TopologyMetaData tm = metaData.cachedOnlyTokenMap();
            endpoints = new ArrayList<InetAddress>(calculateReplicationEndpoints(hostId, tm));
            cachedEndpoints.put(hostId, endpoints);
        }

        return new ArrayList<InetAddress>(endpoints);
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

    protected void validateReplicationFactor(String rf) throws ConfigurationException {
        try {
            if (Integer.parseInt(rf) < 0) {
                throw new ConfigurationException("Replication factor must be non-negative; found " + rf);
            }
        } catch (NumberFormatException e2) {
            throw new ConfigurationException("Replication factor must be numeric; found " + rf);
        }
    }

    private void validateExpectedOptions() throws ConfigurationException {
        Collection<?> expectedOptions = recognizedOptions();
        if (expectedOptions == null)
            return;

        for (String key : configOptions.keySet()) {
            if (!expectedOptions.contains(key))
                throw new ConfigurationException(
                        String.format("Unrecognized strategy option {%s} passed to %s for keyspace %s", key,
                                getClass().getSimpleName(), dbName));
        }
    }

    private static AbstractReplicationStrategy createInternal(String dbName,
            Class<? extends AbstractReplicationStrategy> strategyClass, TopologyMetaData metaData,
            IEndpointSnitch snitch, Map<String, String> strategyOptions) throws ConfigurationException {
        AbstractReplicationStrategy strategy;
        Class<?>[] parameterTypes = new Class[] { String.class, TopologyMetaData.class, IEndpointSnitch.class,
                Map.class };
        try {
            Constructor<? extends AbstractReplicationStrategy> constructor = strategyClass
                    .getConstructor(parameterTypes);
            strategy = constructor.newInstance(dbName, metaData, snitch, strategyOptions);
        } catch (Exception e) {
            throw new ConfigurationException("Error constructing replication strategy class", e);
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
            } catch (ConfigurationException e) {
                logger.warn("Ignoring {}", e.getMessage());
            }

            strategy.validateOptions();
            return strategy;
        } catch (ConfigurationException e) {
            // If that happens at this point, there is nothing we can do about it.
            throw new RuntimeException(e);
        }
    }

    public static void validateReplicationStrategy(String dbName,
            Class<? extends AbstractReplicationStrategy> strategyClass, TopologyMetaData metaData,
            IEndpointSnitch snitch, Map<String, String> strategyOptions) throws ConfigurationException {
        AbstractReplicationStrategy strategy = createInternal(dbName, strategyClass, metaData, snitch, strategyOptions);
        strategy.validateExpectedOptions();
        strategy.validateOptions();
    }

    public static Class<AbstractReplicationStrategy> getClass(String cls) throws ConfigurationException {
        String className = cls.contains(".") ? cls
                : AbstractReplicationStrategy.class.getPackage().getName() + "." + cls;
        Class<AbstractReplicationStrategy> strategyClass = Utils.classForName(className, "replication strategy");
        if (!AbstractReplicationStrategy.class.isAssignableFrom(strategyClass)) {
            throw new ConfigurationException(String.format(
                    "Specified replication strategy class (%s) is not derived from AbstractReplicationStrategy",
                    className));
        }
        return strategyClass;
    }
}

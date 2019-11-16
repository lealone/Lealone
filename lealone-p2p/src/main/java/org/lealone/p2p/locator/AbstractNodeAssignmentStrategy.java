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

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetNode;
import org.lealone.p2p.server.P2pServer;
import org.lealone.p2p.util.Utils;

/**
 * A abstract parent for all node assignment strategies.
*/
public abstract class AbstractNodeAssignmentStrategy {

    private static final Logger logger = LoggerFactory.getLogger(AbstractNodeAssignmentStrategy.class);

    protected final String dbName;
    protected final Map<String, String> configOptions;

    AbstractNodeAssignmentStrategy(String dbName, INodeSnitch snitch, Map<String, String> configOptions) {
        assert dbName != null;
        assert snitch != null;
        this.dbName = dbName;
        this.configOptions = configOptions == null ? Collections.<String, String> emptyMap() : configOptions;
    }

    /**
     * calculate the AF based on strategy_options. When overwriting, ensure that this get()
     *  is FAST, as this is called often.
     *
     * @return the assignment factor
     */
    public abstract int getAssignmentFactor();

    public abstract void validateOptions() throws ConfigException;

    /**
     * The options recognized by the strategy.
     * The empty collection means that no options are accepted, but null means
     * that any option is accepted.
     */
    public abstract Collection<String> recognizedOptions();

    /**
     * calculate the natural nodes
     *
     */
    public abstract List<NetNode> assignNodes(TopologyMetaData metaData, Set<NetNode> oldNodes,
            Set<NetNode> candidateNodes, boolean includeOldNodes);

    public List<NetNode> assignNodes(Set<NetNode> oldNodes, Set<NetNode> candidateNodes, boolean includeOldNodes) {
        TopologyMetaData tm = P2pServer.instance.getTopologyMetaData().getCacheOnlyHostIdMap();
        return assignNodes(tm, oldNodes, candidateNodes, includeOldNodes);
    }

    protected void validateAssignmentFactor(String af) throws ConfigException {
        try {
            if (Integer.parseInt(af) < 0) {
                throw new ConfigException("Assignment factor must be non-negative; found " + af);
            }
        } catch (NumberFormatException e2) {
            throw new ConfigException("Assignment factor must be numeric; found " + af);
        }
    }

    private void validateExpectedOptions() throws ConfigException {
        Collection<?> expectedOptions = recognizedOptions();
        if (expectedOptions == null)
            return;

        for (String key : configOptions.keySet()) {
            key = key.toLowerCase();
            if (!expectedOptions.contains(key))
                throw new ConfigException(
                        String.format("Unrecognized strategy option {%s} passed to %s for database %s", key,
                                getClass().getSimpleName(), dbName));
        }
    }

    private static AbstractNodeAssignmentStrategy createInternal(String dbName,
            Class<? extends AbstractNodeAssignmentStrategy> strategyClass, INodeSnitch snitch,
            Map<String, String> strategyOptions) throws ConfigException {
        AbstractNodeAssignmentStrategy strategy;
        Class<?>[] parameterTypes = new Class[] { String.class, INodeSnitch.class, Map.class };
        try {
            Constructor<? extends AbstractNodeAssignmentStrategy> constructor = strategyClass
                    .getConstructor(parameterTypes);
            strategy = constructor.newInstance(dbName, snitch, strategyOptions);
        } catch (Exception e) {
            throw new ConfigException("Error constructing node assignment strategy class", e);
        }
        return strategy;
    }

    public static AbstractNodeAssignmentStrategy create(String dbName,
            Class<? extends AbstractNodeAssignmentStrategy> strategyClass, INodeSnitch snitch,
            Map<String, String> strategyOptions) {
        try {
            AbstractNodeAssignmentStrategy strategy = createInternal(dbName, strategyClass, snitch, strategyOptions);

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

    public static AbstractNodeAssignmentStrategy create(String dbName, String strategyClassName, INodeSnitch snitch,
            Map<String, String> strategyOptions) {
        Class<? extends AbstractNodeAssignmentStrategy> strategyClass = getClass(strategyClassName);
        return create(dbName, strategyClass, snitch, strategyOptions);
    }

    public static Class<AbstractNodeAssignmentStrategy> getClass(String cls) throws ConfigException {
        String className = cls.contains(".") ? cls
                : AbstractNodeAssignmentStrategy.class.getPackage().getName() + "." + cls;
        Class<AbstractNodeAssignmentStrategy> strategyClass = Utils.classForName(className, "node assignment strategy");
        if (!AbstractNodeAssignmentStrategy.class.isAssignableFrom(strategyClass)) {
            throw new ConfigException(String.format("Specified node assignment strategy class (%s) "
                    + "is not derived from AbstractNodeAssignmentStrategy", className));
        }
        return strategyClass;
    }

    protected static void getFromOldNodes(Set<NetNode> oldNodes, List<NetNode> nodes, int need) {
        Iterator<NetNode> old = oldNodes.iterator();
        while (nodes.size() < need && old.hasNext()) {
            NetNode ep = old.next();
            if (!nodes.contains(ep))
                nodes.add(ep);
        }
    }
}

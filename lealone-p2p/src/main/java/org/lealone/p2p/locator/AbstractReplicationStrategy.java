/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetNode;
import org.lealone.p2p.util.Utils;

/**
 * A abstract parent for all replication strategies.
*/
public abstract class AbstractReplicationStrategy {

    private static final Logger logger = LoggerFactory.getLogger(AbstractReplicationStrategy.class);

    protected final String dbName;
    protected final Map<String, String> configOptions;

    AbstractReplicationStrategy(String dbName, INodeSnitch snitch, Map<String, String> configOptions) {
        assert dbName != null;
        assert snitch != null;
        this.dbName = dbName;
        this.configOptions = configOptions == null ? Collections.<String, String> emptyMap() : configOptions;
    }

    /**
     * calculate the RF based on strategy_options. When overwriting, ensure that this get()
     *  is FAST, as this is called often.
     *
     * @return the replication factor
     */
    public abstract int getReplicationFactor();

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
    public abstract List<NetNode> calculateReplicationNodes(TopologyMetaData metaData, Set<NetNode> oldReplicationNodes,
            Set<NetNode> candidateNodes, boolean includeOldReplicationNodes);

    public List<NetNode> getReplicationNodes(TopologyMetaData metaData, Set<NetNode> oldReplicationNodes,
            Set<NetNode> candidateNodes, boolean includeOldReplicationNodes) {
        TopologyMetaData tm = metaData.getCacheOnlyHostIdMap();
        return calculateReplicationNodes(tm, oldReplicationNodes, candidateNodes, includeOldReplicationNodes);
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
            key = key.toLowerCase();
            if (!expectedOptions.contains(key))
                throw new ConfigException(
                        String.format("Unrecognized strategy option {%s} passed to %s for database %s", key,
                                getClass().getSimpleName(), dbName));
        }
    }

    private static AbstractReplicationStrategy createInternal(String dbName,
            Class<? extends AbstractReplicationStrategy> strategyClass, INodeSnitch snitch,
            Map<String, String> strategyOptions) throws ConfigException {
        AbstractReplicationStrategy strategy;
        Class<?>[] parameterTypes = new Class[] { String.class, INodeSnitch.class, Map.class };
        try {
            Constructor<? extends AbstractReplicationStrategy> constructor = strategyClass
                    .getConstructor(parameterTypes);
            strategy = constructor.newInstance(dbName, snitch, strategyOptions);
        } catch (Exception e) {
            throw new ConfigException("Error constructing replication strategy class", e);
        }
        return strategy;
    }

    public static AbstractReplicationStrategy create(String dbName,
            Class<? extends AbstractReplicationStrategy> strategyClass, INodeSnitch snitch,
            Map<String, String> strategyOptions, boolean validate) {
        try {
            AbstractReplicationStrategy strategy = createInternal(dbName, strategyClass, snitch, strategyOptions);

            // Because we used to not properly validate unrecognized options, we only log a warning if we find one.
            if (validate) {
                try {
                    strategy.validateExpectedOptions();
                } catch (ConfigException e) {
                    logger.warn("Ignoring {}", e.getMessage());
                }
                strategy.validateOptions();
            }
            return strategy;
        } catch (ConfigException e) {
            // If that happens at this point, there is nothing we can do about it.
            throw new RuntimeException(e);
        }
    }

    public static AbstractReplicationStrategy create(String dbName, String strategyClassName, INodeSnitch snitch,
            Map<String, String> strategyOptions, boolean validate) {
        Class<? extends AbstractReplicationStrategy> strategyClass = getClass(strategyClassName);
        return create(dbName, strategyClass, snitch, strategyOptions, validate);
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

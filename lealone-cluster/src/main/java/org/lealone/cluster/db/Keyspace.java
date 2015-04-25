package org.lealone.cluster.db;

import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.locator.AbstractReplicationStrategy;

public class Keyspace {
    private static Keyspace INSTANCE = new Keyspace();

    public static Keyspace open(String keyspaceName) {
        return INSTANCE;
    }

    public static Iterable<Keyspace> all() {
        return null;
    }

    private final AbstractReplicationStrategy replicationStrategy;

    public Keyspace() {
        //TODO 按表或按数据库配置
        replicationStrategy = DatabaseDescriptor.getDefaultReplicationStrategy();
    }

    public AbstractReplicationStrategy getReplicationStrategy() {
        return replicationStrategy;
    }

    public String getName() {
        return null;
    }
}

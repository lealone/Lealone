package org.lealone.cluster.db;

import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.locator.AbstractReplicationStrategy;

public class Keyspace {

    public static Keyspace open(String keyspaceName) {
        return INSTANCE;
    }

    public static Keyspace INSTANCE = new Keyspace();

    AbstractReplicationStrategy replicationStrategy;

    public Keyspace() {
        //TODO 按表或按数据库配置
        replicationStrategy = DatabaseDescriptor.getDefaultReplicationStrategy();
    }

    public AbstractReplicationStrategy getReplicationStrategy() {
        return replicationStrategy;
    }

    public static Iterable<Keyspace> all() {
        return null;
    }

    public String getName() {
        return null;
    }
}

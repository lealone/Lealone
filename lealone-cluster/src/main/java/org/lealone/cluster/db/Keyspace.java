package org.lealone.cluster.db;

import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.locator.AbstractReplicationStrategy;
import org.lealone.cluster.locator.NetworkTopologyStrategy;
import org.lealone.cluster.locator.SimpleStrategy;
import org.lealone.cluster.service.StorageService;

import com.google.common.collect.ImmutableMap;

public class Keyspace {

    public static Keyspace open(String keyspaceName) {
        return INSTANCE;
    }

    public static Keyspace INSTANCE = new Keyspace();

    AbstractReplicationStrategy replicationStrategy;

    public Keyspace() {
        //TODO 按表或按数据配置
        replicationStrategy = new SimpleStrategy("system", StorageService.instance.getTokenMetadata(),
                DatabaseDescriptor.getEndpointSnitch(), ImmutableMap.of("replication_factor", "3"));

        replicationStrategy = new NetworkTopologyStrategy("system", StorageService.instance.getTokenMetadata(),
                DatabaseDescriptor.getEndpointSnitch(), ImmutableMap.of("DC2", "2", "DC1", "1"));
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

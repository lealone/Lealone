package org.lealone.cluster.db;

import java.io.IOException;

import org.lealone.cluster.locator.AbstractReplicationStrategy;

public class Keyspace {

    public static Keyspace open(String keyspaceName) {
        return null;
    }

    public AbstractReplicationStrategy getReplicationStrategy() {
        return null;
    }

    public static Iterable<Keyspace> all() {
        return null;
    }

    public boolean snapshotExists(String snapshotName) {
        return false;
    }

    public void snapshot(String snapshotName, String columnFamilyName) throws IOException {

    }

    public static void clearSnapshot(String snapshotName, String keyspace) {

    }

    public String getName() {
        return null;
    }
}

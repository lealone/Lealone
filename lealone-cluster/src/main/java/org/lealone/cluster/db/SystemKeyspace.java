package org.lealone.cluster.db;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.lealone.cluster.dht.Token;

import com.google.common.collect.SetMultimap;

public class SystemKeyspace {
    public enum BootstrapState {
        NEEDS_BOOTSTRAP, COMPLETED, IN_PROGRESS
    }

    public static Map<InetAddress, Map<String, String>> loadDcRackInfo() {
        return null;
    }

    public static InetAddress getPreferredIP(InetAddress ep) {
        return ep;
    }

    public static synchronized void updatePreferredIP(InetAddress ep, InetAddress preferred_ip) {

    }

    public static boolean bootstrapComplete() {
        return false;
    }

    public static UUID setLocalHostId(UUID hostId) {
        return hostId;
    }

    public static SetMultimap<InetAddress, Token> loadTokens() {
        return null;
    }

    public static Map<InetAddress, UUID> loadHostIds() {
        return null;
    }

    public static synchronized void removeEndpoint(InetAddress ep) {
    }

    public static Collection<Token> getSavedTokens() {
        return null;
    }

    public static UUID getLocalHostId() {
        return null;
    }

    public static int incrementAndGetGeneration() {
        return 1;
    }

    public static boolean bootstrapInProgress() {
        return true;
    }

    public static void setBootstrapState(BootstrapState state) {

    }
}

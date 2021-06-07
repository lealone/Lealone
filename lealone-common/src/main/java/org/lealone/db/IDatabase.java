/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.db.session.Session;
import org.lealone.net.NetNode;
import org.lealone.storage.Storage;
import org.lealone.storage.replication.ReplicationSession;

public interface IDatabase {

    int getId();

    String getShortName();

    String getSysMapName();

    void notifyRunModeChanged();

    Session createInternalSession();

    Session createInternalSession(boolean useSystemDatabase);

    String[] getHostIds();

    void setHostIds(String[] hostIds);

    ReplicationSession createReplicationSession(Session session, Collection<NetNode> replicationNodes);

    ReplicationSession createReplicationSession(Session session, Collection<NetNode> replicationNodes, Boolean remote);

    NetNode getNode(String hostId);

    String getHostId(NetNode node);

    String getLocalHostId();

    List<NetNode> getReplicationNodes(Set<NetNode> oldReplicationNodes, Set<NetNode> candidateNodes);

    boolean isShardingMode();

    Map<String, String> getParameters();

    Map<String, String> getReplicationParameters();

    Map<String, String> getNodeAssignmentParameters();

    List<Storage> getStorages();

    RunMode getRunMode();

    boolean isStarting();

    String getCreateSQL();

    IDatabase copy();
}

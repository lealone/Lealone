/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.util.List;
import java.util.Map;

import org.lealone.net.NetNode;
import org.lealone.storage.Storage;

public interface IDatabase {

    int getId();

    String getShortName();

    String getSysMapName();

    void notifyRunModeChanged();

    String[] getHostIds();

    void setHostIds(String[] hostIds);

    NetNode getNode(String hostId);

    String getHostId(NetNode node);

    String getLocalHostId();

    Map<String, String> getParameters();

    Map<String, String> getReplicationParameters();

    Map<String, String> getNodeAssignmentParameters();

    List<Storage> getStorages();

    RunMode getRunMode();

    boolean isStarting();

    String getCreateSQL();

    IDatabase copy();
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.lealone.net.NetNode;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.util.Utils;

public class NodeSnitchInfo implements NodeSnitchInfoMBean {
    public static void create() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(new NodeSnitchInfo(), new ObjectName(Utils.getJmxObjectName("NodeSnitchInfo")));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDatacenter(String host) throws UnknownHostException {
        return ConfigDescriptor.getNodeSnitch().getDatacenter(NetNode.getByName(host));
    }

    @Override
    public String getRack(String host) throws UnknownHostException {
        return ConfigDescriptor.getNodeSnitch().getRack(NetNode.getByName(host));
    }

    @Override
    public String getSnitchName() {
        return ConfigDescriptor.getNodeSnitch().getClass().getName();
    }
}

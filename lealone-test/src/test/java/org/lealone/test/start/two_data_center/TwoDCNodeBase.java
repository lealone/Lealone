/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start.two_data_center;

import org.lealone.p2p.locator.SnitchProperties;
import org.lealone.test.start.NodeBase;

public class TwoDCNodeBase extends NodeBase {
    public static void run(String rackdcPropertyFileName, Class<?> loader, String[] args) {
        System.setProperty(SnitchProperties.RACKDC_PROPERTY_FILENAME, rackdcPropertyFileName);
        NodeBase.run(loader, args);
    }

    public TwoDCNodeBase() {
        nodeBaseDirPrefix = "cluster/twodc-";
        node_snitch = "GossipingPropertyFileSnitch";
    }
}

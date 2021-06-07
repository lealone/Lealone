/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import org.lealone.net.NetNode;

public interface ILatencySubscriber {
    public void receiveTiming(NetNode address, long latency);
}

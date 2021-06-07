/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

public class NetNodeManagerHolder {

    private static NetNodeManager netNodeManager = LocalNetNodeManager.getInstance();

    public static NetNodeManager get() {
        return netNodeManager;
    }

    public static void set(NetNodeManager m) {
        if (m == null)
            throw new NullPointerException("NetNodeManager is null");
        netNodeManager = m;
    }
}

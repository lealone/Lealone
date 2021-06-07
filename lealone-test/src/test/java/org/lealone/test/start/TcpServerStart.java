/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start;

import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.p2p.config.Config;
import org.lealone.p2p.config.Config.PluggableEngineDef;
import org.lealone.p2p.server.P2pServerEngine;

public class TcpServerStart extends NodeBase {

    // YamlConfigLoader的子类必须有一个无参数的构造函数
    public TcpServerStart() {
        nodeBaseDirPrefix = "client-server";
    }

    @Override
    public void applyConfig(Config config) throws ConfigException {
        for (PluggableEngineDef e : config.protocol_server_engines) {
            if (P2pServerEngine.NAME.equalsIgnoreCase(e.name)) {
                e.enabled = false;
            }
        }
        super.applyConfig(config);
    }

    public static void main(String[] args) {
        NodeBase.run(TcpServerStart.class, args);
    }

    public static void run() {
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            NodeBase.run(TcpServerStart.class, new String[0], latch);
        }).start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

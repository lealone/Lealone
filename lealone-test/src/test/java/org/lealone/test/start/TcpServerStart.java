/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start;

import java.util.concurrent.CountDownLatch;

public class TcpServerStart extends NodeBase {

    // YamlConfigLoader的子类必须有一个无参数的构造函数
    public TcpServerStart() {
        nodeBaseDirPrefix = "client-server";
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

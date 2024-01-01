/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.util;

public class ThreadUtils {

    public static void start(String name, Runnable target) {
        Thread t = new Thread(target, name);
        t.setDaemon(true);
        t.start();
    }

    public static void start(String name, boolean daemon, Runnable target) {
        Thread t = new Thread(target, name);
        t.setDaemon(daemon);
        t.start();
    }

}

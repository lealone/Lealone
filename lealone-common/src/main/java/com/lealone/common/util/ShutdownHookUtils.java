/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.util;

public class ShutdownHookUtils {

    private static Thread globalShutdownHook;
    private static int globalLevel = Integer.MAX_VALUE;

    public static Thread getGlobalShutdownHook() {
        return globalShutdownHook;
    }

    public static synchronized void setGlobalShutdownHook(int level, Class<?> clz, Runnable runnable) {
        if (globalLevel == Integer.MAX_VALUE) {
            globalShutdownHook = addShutdownHook(clz, runnable);
            globalLevel = level;
        } else if (level < globalLevel) {
            Runtime.getRuntime().removeShutdownHook(globalShutdownHook);
            globalLevel = level;
        }
    }

    public static synchronized Thread addShutdownHook(Object object, Runnable runnable) {
        return addShutdownHook(object.getClass(), runnable);
    }

    private static synchronized Thread addShutdownHook(Class<?> clz, Runnable runnable) {
        String hookName = clz.getSimpleName() + "-ShutdownHook";
        Thread hook = new Thread(runnable, hookName);
        Runtime.getRuntime().addShutdownHook(hook);
        return hook;
    }
}

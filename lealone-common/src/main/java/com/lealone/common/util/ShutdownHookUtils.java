/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.util;

import java.util.HashSet;

public class ShutdownHookUtils {

    private static final HashSet<Thread> hooks = new HashSet<>();

    public static synchronized Thread addShutdownHook(Object object, Runnable hook) {
        String hookName = object.getClass().getSimpleName() + "-ShutdownHook-" + hooks.size();
        return addShutdownHook(hookName, hook, false);
    }

    public static synchronized Thread addShutdownHook(String hookName, Runnable hook) {
        return addShutdownHook(hookName, hook, true);
    }

    public static synchronized Thread addShutdownHook(String hookName, Runnable hook,
            boolean addPostfix) {
        if (addPostfix)
            hookName += "-ShutdownHook";
        Thread t = new Thread(hook, hookName);
        return addShutdownHook(t);
    }

    public static synchronized Thread addShutdownHook(Thread hook) {
        hooks.add(hook);
        Runtime.getRuntime().addShutdownHook(hook);
        return hook;
    }

    public static synchronized void removeShutdownHook(Thread hook) {
        hooks.remove(hook);
        Runtime.getRuntime().removeShutdownHook(hook);
    }

    public static synchronized void removeAllShutdownHooks() {
        for (Thread hook : hooks) {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
        hooks.clear();
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.concurrent;

/**
 * Centralized location for shared executors
 */
public class ScheduledExecutors {
    /**
     * This pool is used for periodic short (sub-second) tasks.
     */
    public static final DebuggableScheduledThreadPoolExecutor scheduledTasks = new DebuggableScheduledThreadPoolExecutor(
            "ScheduledTasks");
}

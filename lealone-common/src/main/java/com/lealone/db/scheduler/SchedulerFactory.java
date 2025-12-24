/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.scheduler;

import java.util.Map;

import com.lealone.db.plugin.Plugin;

public interface SchedulerFactory extends Plugin {

    Scheduler getScheduler();

    Scheduler getScheduler(int id);

    Scheduler[] getSchedulers();

    int getSchedulerCount();

    public static SchedulerFactory getDefaultSchedulerFactory() {
        return SchedulerFactoryBase.getDefaultSchedulerFactory();
    }

    public static SchedulerFactory getSchedulerFactory(Class<? extends Scheduler> schedulerClass,
            Map<String, String> config) {
        return getSchedulerFactory(schedulerClass, config, true);
    }

    public static SchedulerFactory getSchedulerFactory(Class<? extends Scheduler> schedulerClass,
            Map<String, String> config, boolean startFactory) {
        return SchedulerFactoryBase.getSchedulerFactory(schedulerClass, config, startFactory);
    }
}

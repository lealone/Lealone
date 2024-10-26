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

    Scheduler bindScheduler(Thread thread);

    public static SchedulerFactory create(Map<String, String> config) {
        return create(config, null);
    }

    public static SchedulerFactory create(Map<String, String> config, Scheduler[] schedulers) {
        return SchedulerFactoryBase.create(config, schedulers);
    }

    public static void setDefaultSchedulerFactory(SchedulerFactory defaultSchedulerFactory) {
        SchedulerFactoryBase.setDefaultSchedulerFactory(defaultSchedulerFactory);
    }

    public static SchedulerFactory getDefaultSchedulerFactory() {
        return SchedulerFactoryBase.getDefaultSchedulerFactory();
    }

    public static SchedulerFactory getDefaultSchedulerFactory(String schedulerClassName,
            Map<String, String> config) {
        return SchedulerFactoryBase.getDefaultSchedulerFactory(schedulerClassName, config);
    }

    public static SchedulerFactory initDefaultSchedulerFactory(String schedulerClassName,
            Map<String, String> config) {
        return SchedulerFactoryBase.initDefaultSchedulerFactory(schedulerClassName, config);
    }
}

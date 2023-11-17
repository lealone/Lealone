/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.util.Map;

import org.lealone.db.Plugin;
import org.lealone.storage.page.PageOperationHandlerFactory;

public interface SchedulerFactory extends Plugin, PageOperationHandlerFactory {

    Scheduler getScheduler();

    int getSchedulerCount();

    public static SchedulerFactory create(Map<String, String> config) {
        return create(config, null);
    }

    public static SchedulerFactory create(Map<String, String> config, Scheduler[] schedulers) {
        return SchedulerFactoryBase.create(config, schedulers);
    }
}

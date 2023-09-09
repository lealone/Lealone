/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.util.MapUtils;
import org.lealone.db.PluginManager;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.page.PageOperationHandlerFactory;

public class SchedulerFactory {

    private static Scheduler[] schedulers;
    private static final AtomicInteger index = new AtomicInteger(0);

    public static Scheduler getScheduler() {
        return schedulers[PageOperationHandlerFactory.getAndIncrementIndex(index) % schedulers.length];
    }

    public static int getSchedulerCount() {
        return schedulers.length;
    }

    public static synchronized void init(Map<String, String> config) {
        if (schedulers != null)
            return;
        int schedulerCount = MapUtils.getSchedulerCount(config);
        schedulers = new Scheduler[schedulerCount];
        for (int i = 0; i < schedulerCount; i++) {
            schedulers[i] = new Scheduler(i, schedulerCount, config);
        }

        PageOperationHandlerFactory pohFactory = PageOperationHandlerFactory.create(config, schedulers);
        for (StorageEngine e : PluginManager.getPlugins(StorageEngine.class)) {
            // 停掉旧的处理器，不能同时有两个
            PageOperationHandlerFactory factory = e.getPageOperationHandlerFactory();
            if (factory != null && factory != pohFactory) {
                factory.stopHandlers();
            }
            e.setPageOperationHandlerFactory(pohFactory);
        }

        // 提前启动，LealoneDatabase要用到存储引擎
        for (Scheduler scheduler : schedulers) {
            scheduler.start();
        }
    }

    public static synchronized void destroy() {
        if (schedulers == null)
            return;
        for (Scheduler scheduler : schedulers) {
            scheduler.end();
        }
        for (Scheduler scheduler : schedulers) {
            if (Thread.currentThread() != scheduler) {
                try {
                    scheduler.join();
                } catch (InterruptedException e) {
                }
            }
        }
        schedulers = null;
    }
}

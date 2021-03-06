/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.db.async.AsyncTaskHandlerFactory;
import org.lealone.storage.PageOperationHandlerFactory;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.StorageEngineManager;

public class ScheduleService {

    private static Scheduler[] schedulers;
    private static final AtomicInteger index = new AtomicInteger(0);
    private static final AtomicInteger indexForSession = new AtomicInteger(0);

    static void init(Map<String, String> config) {
        int schedulerCount;
        if (config.containsKey("scheduler_count"))
            schedulerCount = Integer.parseInt(config.get("scheduler_count"));
        else
            schedulerCount = Math.max(1, Runtime.getRuntime().availableProcessors());

        schedulers = new Scheduler[schedulerCount];
        for (int i = 0; i < schedulerCount; i++) {
            schedulers[i] = new Scheduler(i, config);
        }

        AsyncTaskHandlerFactory.setAsyncTaskHandlers(schedulers);
        PageOperationHandlerFactory pohFactory = PageOperationHandlerFactory.create(config, schedulers);
        for (StorageEngine e : StorageEngineManager.getInstance().getEngines()) {
            e.setPageOperationHandlerFactory(pohFactory);
        }
    }

    static void start() {
        for (Scheduler scheduler : schedulers) {
            scheduler.start();
        }
    }

    static void stop() {
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
    }

    static Scheduler getScheduler() {
        return schedulers[index.getAndIncrement() % schedulers.length];
    }

    static Scheduler getSchedulerForSession() {
        return schedulers[indexForSession.getAndIncrement() % schedulers.length];
    }
}

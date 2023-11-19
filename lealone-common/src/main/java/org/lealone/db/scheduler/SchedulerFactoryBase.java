/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.util.MapUtils;
import org.lealone.db.Plugin;
import org.lealone.db.PluginBase;
import org.lealone.db.async.AsyncTaskHandlerFactory;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.page.PageOperationHandler;

public abstract class SchedulerFactoryBase extends PluginBase implements SchedulerFactory {

    protected Scheduler[] schedulers = new Scheduler[0];

    protected SchedulerFactoryBase(Map<String, String> config, Scheduler[] schedulers) {
        super("SchedulerFactory");
        boolean embedded = false;
        if (schedulers != null) {
            StorageEngine se = StorageEngine.getDefaultStorageEngine();
            if (se != null)
                se.setPageOperationHandlerFactory(this);
        } else {
            // 如果未指定调度器，那么使用嵌入式调度器
            schedulers = EmbeddedScheduler.createSchedulers(config);
            embedded = true;
        }
        init(config);
        for (Scheduler scheduler : schedulers) {
            scheduler.setSchedulerFactory(this);
        }
        AsyncTaskHandlerFactory.setAsyncTaskHandlers(schedulers);
        this.schedulers = schedulers;
        if (embedded) // 嵌入式场景自动启动调度器
            start();
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return SchedulerFactory.class;
    }

    @Override
    public int getSchedulerCount() {
        return schedulers.length;
    }

    @Override
    public PageOperationHandler getPageOperationHandler() {
        return getScheduler();
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        Scheduler[] schedulers = this.schedulers;
        for (Scheduler scheduler : schedulers) {
            scheduler.start();
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        Scheduler[] schedulers = this.schedulers;
        for (Scheduler scheduler : schedulers) {
            scheduler.stop();
        }
        for (Scheduler scheduler : schedulers) {
            Thread thread = scheduler.getThread();
            if (Thread.currentThread() != thread) {
                try {
                    if (thread != null)
                        thread.join();
                } catch (InterruptedException e) {
                }
            }
        }
        this.schedulers = new Scheduler[0];
        super.stop();
    }

    public static SchedulerFactory create(Map<String, String> config) {
        return create(config, null);
    }

    public static synchronized SchedulerFactory create(Map<String, String> config,
            Scheduler[] schedulers) {
        SchedulerFactory factory = null;
        String key = "scheduler_factory_type";
        String type = MapUtils.getString(config, key, null);
        if (type == null || type.equalsIgnoreCase("RoundRobin"))
            factory = new RoundRobinFactory(config, schedulers);
        else if (type.equalsIgnoreCase("Random"))
            factory = new RandomFactory(config, schedulers);
        else if (type.equalsIgnoreCase("LoadBalance"))
            factory = new LoadBalanceFactory(config, schedulers);
        else {
            throw new RuntimeException("Unknow " + key + ": " + type);
        }
        return factory;
    }

    private static class RandomFactory extends SchedulerFactoryBase {

        private static final Random random = new Random();

        protected RandomFactory(Map<String, String> config, Scheduler[] schedulers) {
            super(config, schedulers);
        }

        @Override
        public Scheduler getScheduler() {
            int index = random.nextInt(schedulers.length);
            return schedulers[index];
        }
    }

    private static class RoundRobinFactory extends SchedulerFactoryBase {

        private static final AtomicInteger index = new AtomicInteger(0);

        protected RoundRobinFactory(Map<String, String> config, Scheduler[] schedulers) {
            super(config, schedulers);
        }

        @Override
        public Scheduler getScheduler() {
            return schedulers[getAndIncrementIndex(index) % schedulers.length];
        }
    }

    private static class LoadBalanceFactory extends SchedulerFactoryBase {

        protected LoadBalanceFactory(Map<String, String> config, Scheduler[] schedulers) {
            super(config, schedulers);
        }

        @Override
        public Scheduler getScheduler() {
            long minLoad = Long.MAX_VALUE;
            int index = 0;
            for (int i = 0, size = schedulers.length; i < size; i++) {
                long load = schedulers[i].getLoad();
                if (load < minLoad) {
                    index = i;
                    minLoad = load;
                }
            }
            return schedulers[index];
        }
    }

    // 变成负数时从0开始
    public static int getAndIncrementIndex(AtomicInteger index) {
        int i = index.getAndIncrement();
        if (i < 0) {
            if (index.compareAndSet(i, 1)) {
                i = 0;
            } else {
                i = index.getAndIncrement();
            }
        }
        return i;
    }
}

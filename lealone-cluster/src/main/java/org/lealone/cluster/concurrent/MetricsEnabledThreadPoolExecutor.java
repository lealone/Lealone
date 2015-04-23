/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.cluster.concurrent;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.lealone.cluster.metrics.ThreadPoolMetrics;

/**
 * This is a wrapper class for the <i>ScheduledThreadPoolExecutor</i>. It provides an implementation
 * for the <i>afterExecute()</i> found in the <i>ThreadPoolExecutor</i> class to log any unexpected
 * Runtime Exceptions.
 */

public class MetricsEnabledThreadPoolExecutor extends DebuggableThreadPoolExecutor {
    private final ThreadPoolMetrics metrics;

    public MetricsEnabledThreadPoolExecutor(String threadPoolName) {
        this(1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(
                threadPoolName), "internal");
    }

    public MetricsEnabledThreadPoolExecutor(String threadPoolName, String jmxPath) {
        this(1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(
                threadPoolName), jmxPath);
    }

    public MetricsEnabledThreadPoolExecutor(String threadPoolName, int priority) {
        this(1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(
                threadPoolName, priority), "internal");
    }

    public MetricsEnabledThreadPoolExecutor(int corePoolSize, long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> workQueue, NamedThreadFactory threadFactory, String jmxPath) {
        this(corePoolSize, corePoolSize, keepAliveTime, unit, workQueue, threadFactory, jmxPath);
    }

    public MetricsEnabledThreadPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> workQueue, NamedThreadFactory threadFactory, String jmxPath) {
        super(corePoolSize, maxPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        super.prestartAllCoreThreads();

        metrics = new ThreadPoolMetrics(this, jmxPath, threadFactory.id);
    }

    public MetricsEnabledThreadPoolExecutor(Stage stage) {
        this(stage.getJmxName(), stage.getJmxType());
    }

    private void unregisterMBean() {
        // release metrics
        metrics.release();
    }

    @Override
    public synchronized void shutdown() {
        // synchronized, because there is no way to access super.mainLock, which would be
        // the preferred way to make this threadsafe
        if (!isShutdown()) {
            unregisterMBean();
        }
        super.shutdown();
    }

    @Override
    public synchronized List<Runnable> shutdownNow() {
        // synchronized, because there is no way to access super.mainLock, which would be
        // the preferred way to make this threadsafe
        if (!isShutdown()) {
            unregisterMBean();
        }
        return super.shutdownNow();
    }

    /**
     * Get the number of completed tasks
     */
    public long getCompletedTasks() {
        return getCompletedTaskCount();
    }

    /**
     * Get the number of tasks waiting to be executed
     */
    public long getPendingTasks() {
        return getTaskCount() - getCompletedTaskCount();
    }

    public int getTotalBlockedTasks() {
        return (int) metrics.totalBlocked.count();
    }

    public int getCurrentlyBlockedTasks() {
        return (int) metrics.currentBlocked.count();
    }

    public int getCoreThreads() {
        return getCorePoolSize();
    }

    public void setCoreThreads(int number) {
        setCorePoolSize(number);
    }

    public int getMaximumThreads() {
        return getMaximumPoolSize();
    }

    public void setMaximumThreads(int number) {
        setMaximumPoolSize(number);
    }

    @Override
    protected void onInitialRejection(Runnable task) {
        metrics.totalBlocked.inc();
        metrics.currentBlocked.inc();
    }

    @Override
    protected void onFinalAccept(Runnable task) {
        metrics.currentBlocked.dec();
    }

    @Override
    protected void onFinalRejection(Runnable task) {
        metrics.currentBlocked.dec();
    }
}

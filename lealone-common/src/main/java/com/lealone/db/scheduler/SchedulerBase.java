/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.scheduler;

import java.nio.channels.Selector;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.lealone.common.util.MapUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.RunMode;
import com.lealone.db.async.AsyncPeriodicTask;
import com.lealone.db.async.AsyncTask;
import com.lealone.db.link.LinkableList;
import com.lealone.db.session.Session;
import com.lealone.net.NetBuffer;

public abstract class SchedulerBase implements Scheduler {

    protected final int id;
    protected final String name;

    protected final long loopInterval;
    protected boolean started;
    protected boolean stopped;

    protected SchedulerThread thread;
    protected SchedulerFactory schedulerFactory;

    protected Session currentSession;

    // 执行一些周期性任务，数量不多，以读为主，所以用LinkableList
    // 用LinkableList是安全的，所有的初始PeriodicTask都在main线程中注册，新的PeriodicTask在当前调度线程中注册
    protected final LinkableList<AsyncPeriodicTask> periodicTasks = new LinkableList<>();

    public SchedulerBase(int id, String name, int schedulerCount, Map<String, String> config) {
        this.id = id;
        this.name = name;
        // 默认100毫秒
        loopInterval = MapUtils.getLong(config, "scheduler_loop_interval", 100);

        thread = new SchedulerThread(this);
        thread.setName(name);
        thread.setDaemon(RunMode.isEmbedded(config));
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public long getLoad() {
        return 0;
    }

    @Override
    public SchedulerThread getThread() {
        return thread;
    }

    @Override
    public SchedulerFactory getSchedulerFactory() {
        return schedulerFactory;
    }

    @Override
    public void setSchedulerFactory(SchedulerFactory schedulerFactory) {
        this.schedulerFactory = schedulerFactory;
    }

    @Override
    public synchronized void start() {
        if (started)
            return;
        thread.start();
        started = true;
        stopped = false;
    }

    @Override
    public synchronized void stop() {
        started = false;
        stopped = true;
        wakeUp();
    }

    protected void onStopped() {
        thread = null;
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public Session getCurrentSession() {
        return currentSession;
    }

    @Override
    public void setCurrentSession(Session currentSession) {
        this.currentSession = currentSession;
    }

    @Override
    public void executeNextStatement() {
    }

    @Override
    public Object getNetEventLoop() {
        return null;
    }

    @Override
    public Selector getSelector() {
        return null;
    }

    protected void runEventLoop() {
    }

    protected void runMiscTasks() {
    }

    protected void runMiscTasks(ConcurrentLinkedQueue<AsyncTask> miscTasks) {
        if (!miscTasks.isEmpty()) {
            AsyncTask task = miscTasks.poll();
            while (task != null) {
                try {
                    task.run();
                } catch (Throwable e) {
                    getLogger().warn("Failed to run misc task: " + task, e);
                }
                task = miscTasks.poll();
            }
        }
    }

    // --------------------- 实现 AsyncTaskHandler 相关的代码 ---------------------
    @Override
    public void addPeriodicTask(AsyncPeriodicTask task) {
        periodicTasks.add(task);
    }

    @Override
    public void removePeriodicTask(AsyncPeriodicTask task) {
        periodicTasks.remove(task);
    }

    protected void runPeriodicTasks() {
        if (periodicTasks.isEmpty())
            return;

        AsyncPeriodicTask task = periodicTasks.getHead();
        while (task != null) {
            try {
                task.run();
            } catch (Throwable e) {
                getLogger().warn("Failed to run periodic task: " + task, e);
            }
            task = task.next;
        }
    }

    protected NetBuffer inputBuffer;

    @Override
    public NetBuffer getInputBuffer() {
        if (inputBuffer == null)
            inputBuffer = new NetBuffer(DataBuffer.createDirect());
        return inputBuffer;
    }

    protected NetBuffer outputBuffer;

    @Override
    public NetBuffer getOutputBuffer() {
        if (outputBuffer == null)
            outputBuffer = new NetBuffer(DataBuffer.createDirect());
        return outputBuffer;
    }
}

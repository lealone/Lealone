/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.client;

import java.nio.channels.Selector;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.client.session.ClientSession;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.DataBufferFactory;
import com.lealone.db.async.AsyncTask;
import com.lealone.db.link.LinkableBase;
import com.lealone.db.link.LinkableList;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerBase;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.db.scheduler.SchedulerFactoryBase;
import com.lealone.db.session.Session;
import com.lealone.db.session.SessionInfo;
import com.lealone.net.NetClient;
import com.lealone.net.NetEventLoop;
import com.lealone.net.NetFactory;

public class ClientScheduler extends SchedulerBase {

    private static final Logger logger = LoggerFactory.getLogger(ClientScheduler.class);

    // 杂七杂八的任务，数量不多，执行完就删除
    private final ConcurrentLinkedQueue<AsyncTask> miscTasks = new ConcurrentLinkedQueue<>();
    private final NetClient netClient;
    private final NetEventLoop netEventLoop;
    private final AtomicLong load = new AtomicLong(0);

    public ClientScheduler(int id, int schedulerCount, Map<String, String> config) {
        super(id, "CScheduleService-" + id, schedulerCount, config);
        NetFactory netFactory = NetFactory.getFactory(config);
        netClient = netFactory.createNetClient();
        netEventLoop = netFactory.createNetEventLoop(this, loopInterval);
        netEventLoop.setNetClient(netClient);
        netEventLoop.setPreferBatchWrite(false);
        getThread().setDaemon(true);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public long getLoad() {
        return super.getLoad() + miscTasks.size() + load.get();
    }

    @Override
    public boolean isBusy() {
        return load.get() > 1;
    }

    @Override
    public void handle(AsyncTask task) {
        miscTasks.add(task);
        wakeUp();
    }

    @Override
    protected void runMiscTasks() {
        runMiscTasks(miscTasks);
    }

    @Override
    public void run() {
        long lastTime = System.currentTimeMillis();
        while (!stopped) {
            if (netEventLoop.needWriteImmediately())
                netEventLoop.write();

            runMiscTasks();
            runSessionTasks();
            runEventLoop();

            long currentTime = System.currentTimeMillis();
            if (currentTime - lastTime > 1000) {
                lastTime = currentTime;
                checkTimeout(currentTime);
            }
        }
        onStopped();
    }

    private void checkTimeout(long currentTime) {
        try {
            netClient.checkTimeout(currentTime);
        } catch (Throwable t) {
            logger.warn("Failed to checkTimeout", t);
        }
    }

    @Override
    public void executeNextStatement() {
        runSessionTasks();
        // 客户端阻塞在同步方法时运行事件循环执行回调
        runEventLoop();
    }

    private void runSessionTasks() {
        if (sessions.isEmpty())
            return;
        ClientSessionInfo si = sessions.getHead();
        while (si != null) {
            si.runSessionTasks();
            si = si.next;
        }
    }

    private final LinkableList<ClientSessionInfo> sessions = new LinkableList<>();

    private static class ClientSessionInfo extends LinkableBase<ClientSessionInfo>
            implements SessionInfo {

        private final ClientSession session;
        private final ClientScheduler scheduler;
        private final LinkedBlockingQueue<AsyncTask> tasks = new LinkedBlockingQueue<>();

        public ClientSessionInfo(ClientSession session, ClientScheduler scheduler) {
            this.session = session;
            this.scheduler = scheduler;
            this.session.setSessionInfo(this);
        }

        @Override
        public Session getSession() {
            return session;
        }

        @Override
        public int getSessionId() {
            return session.getId();
        }

        @Override
        public void submitTask(AsyncTask task) {
            tasks.add(task);
            scheduler.load.incrementAndGet();
        }

        void runSessionTasks() {
            if (!tasks.isEmpty()) {
                AsyncTask task = tasks.poll();
                while (task != null) {
                    scheduler.load.decrementAndGet();
                    runTask(task);
                    task = tasks.poll();
                }
            }
        }

        private void runTask(AsyncTask task) {
            Session old = scheduler.getCurrentSession();
            scheduler.setCurrentSession(session);
            try {
                task.run();
            } catch (Throwable e) {
                logger.warn(
                        "Failed to run async session task: " + task + ", session id: " + getSessionId(),
                        e);
            } finally {
                scheduler.setCurrentSession(old);
            }
        }
    }

    // --------------------- 网络事件循环 ---------------------

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return netEventLoop.getDataBufferFactory();
    }

    @Override
    public NetEventLoop getNetEventLoop() {
        return netEventLoop;
    }

    @Override
    public Selector getSelector() {
        return netEventLoop.getSelector();
    }

    @Override
    public void wakeUp() {
        netEventLoop.wakeUp();
    }

    @Override
    protected void runEventLoop() {
        try {
            netEventLoop.write();
            netEventLoop.select();
            netEventLoop.handleSelectedKeys();
        } catch (Throwable t) {
            getLogger().warn("Failed to runEventLoop", t);
        }
    }

    @Override
    protected void onStopped() {
        super.onStopped();
        netEventLoop.close();
    }

    private static volatile SchedulerFactory clientSchedulerFactory;

    public static Scheduler getScheduler(ConnectionInfo ci, Map<String, String> config) {
        if (clientSchedulerFactory == null) {
            synchronized (ClientScheduler.class) {
                if (clientSchedulerFactory == null) {
                    clientSchedulerFactory = SchedulerFactoryBase
                            .createSchedulerFactory(ClientScheduler.class.getName(), config);
                }
            }
        }
        return SchedulerFactoryBase.getScheduler(clientSchedulerFactory, ci);
    }

    @Override
    public void addSession(Session session) {
        sessions.add(new ClientSessionInfo((ClientSession) session, this));
    }

    @Override
    public void removeSession(Session session) {
        if (sessions.isEmpty())
            return;
        ClientSessionInfo si = sessions.getHead();
        while (si != null) {
            if (si.session == session) {
                sessions.remove(si);
                break;
            }
            si = si.next;
        }
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;

import com.lealone.common.util.BitField;
import com.lealone.db.scheduler.Scheduler;

public class AsyncServerManager {

    private static final BitField serverIds = new BitField();
    private static final ArrayList<AsyncServer<?>> servers = new ArrayList<>(1);
    // 注册网络ACCEPT事件，个数会动态增减
    private static RegisterAccepterTask[] registerAccepterTasks = new RegisterAccepterTask[1];

    public static int allocateServerId() {
        synchronized (serverIds) {
            int i = serverIds.nextClearBit(0);
            serverIds.set(i);
            return i;
        }
    }

    public static void clearServerId(int id) {
        synchronized (serverIds) {
            serverIds.clear(id);
        }
    }

    public static boolean isServerIdEnabled(int id) {
        synchronized (serverIds) {
            return serverIds.get(id);
        }
    }

    public static void addServer(AsyncServer<?> server) {
        synchronized (servers) {
            servers.add(server);
            // serverId从0开始
            int serverId = server.getServerId();
            if (serverId >= registerAccepterTasks.length) {
                RegisterAccepterTask[] tasks = new RegisterAccepterTask[serverId + 1];
                System.arraycopy(registerAccepterTasks, 0, tasks, 0, registerAccepterTasks.length);
                tasks[tasks.length - 1] = new RegisterAccepterTask();
                registerAccepterTasks = tasks;
            } else {
                registerAccepterTasks[serverId] = new RegisterAccepterTask();
            }
        }
    }

    public static void removeServer(AsyncServer<?> server) {
        synchronized (servers) {
            servers.remove(server);
            int serverId = server.getServerId();
            // 删除最后一个元素时才需要变动数组
            if (serverId > 0 && serverId == registerAccepterTasks.length - 1) {
                RegisterAccepterTask[] tasks = new RegisterAccepterTask[serverId];
                System.arraycopy(registerAccepterTasks, 0, tasks, 0, serverId);
                registerAccepterTasks = tasks;
            } else {
                registerAccepterTasks[serverId] = null;
            }
            clearServerId(serverId);
        }
    }

    private static class RegisterAccepterTask {

        private AsyncServer<?> asyncServer;
        private Scheduler nextScheduler; // 下一个负责监听Accept事件的调度器
        private boolean needRegisterAccepter; // 避免重复注册Accept事件

        private void run(Scheduler currentScheduler) {
            try {
                SelectionKey key = asyncServer.getServerChannel().register(
                        currentScheduler.getSelector(), SelectionKey.OP_ACCEPT,
                        asyncServer.getProtocolServer());
                asyncServer.setSelectionKey(key);
                needRegisterAccepter = false;
            } catch (ClosedChannelException e) {
                currentScheduler.getLogger()
                        .warn("Failed to register server channel: " + asyncServer.getServerChannel());
            }
        }
    }

    // 注册和轮询TOP_ACCEPT事件的线程必需是同一个，否则会有很诡异的问题
    public static void runRegisterAccepterTasks(Scheduler currentScheduler) {
        RegisterAccepterTask[] tasks = registerAccepterTasks;
        for (int i = 0; i < tasks.length; i++) {
            RegisterAccepterTask task = tasks[i];
            if (task != null && task.needRegisterAccepter && task.nextScheduler == currentScheduler)
                task.run(currentScheduler);
        }
    }

    public static void registerAccepterIfNeed(AsyncServer<?> asyncServer, Scheduler currentScheduler) {
        if (asyncServer.isRoundRobinAcceptEnabled()) {
            Scheduler nextScheduler = currentScheduler.getSchedulerFactory().getScheduler();
            // 如果下一个负责处理网络accept事件的调度器又是当前调度器，那么不需要做什么
            if (nextScheduler != currentScheduler) {
                SelectionKey key = asyncServer.getSelectionKey();
                key.interestOps(key.interestOps() & ~SelectionKey.OP_ACCEPT);
                registerAccepter(asyncServer, nextScheduler);
                nextScheduler.wakeUp();
            }
        }
    }

    public static void registerAccepter(AsyncServer<?> asyncServer, Scheduler nextScheduler) {
        int serverId = asyncServer.getServerId();
        // Server重新启动后对应的元素可能已经删除，需要重新加入
        if (serverId >= registerAccepterTasks.length) {
            addServer(asyncServer);
        }
        RegisterAccepterTask task = registerAccepterTasks[serverId];
        // Server重新启动后对应的元素可能为null，重新创建一个即可
        if (task == null) {
            task = new RegisterAccepterTask();
            registerAccepterTasks[serverId] = task;
        }
        task.asyncServer = asyncServer;
        task.nextScheduler = nextScheduler;
        task.needRegisterAccepter = true;
    }
}

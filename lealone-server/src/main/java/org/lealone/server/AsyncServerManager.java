/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;

import org.lealone.common.util.BitField;

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
        private ServerSocketChannel serverChannel;
        private Scheduler currentScheduler;
        private boolean needRegisterAccepter; // 避免重复注册ACCEPT事件

        private void run(Scheduler currentScheduler) {
            try {
                serverChannel.register(currentScheduler.getNetEventLoop().getSelector(),
                        SelectionKey.OP_ACCEPT, this);
                needRegisterAccepter = false;
            } catch (ClosedChannelException e) {
                currentScheduler.getLogger().warn("Failed to register server channel: " + serverChannel);
            }
        }
    }

    public static void registerAccepter(AsyncServer<?> asyncServer, ServerSocketChannel serverChannel,
            Scheduler currentScheduler) {
        RegisterAccepterTask task = registerAccepterTasks[asyncServer.getServerId()];
        // Server重新启动后对应的元素可能为null，重新创建一个即可
        if (task == null) {
            task = new RegisterAccepterTask();
            registerAccepterTasks[asyncServer.getServerId()] = task;
        }
        task.asyncServer = asyncServer;
        task.serverChannel = serverChannel;
        task.currentScheduler = currentScheduler;
        task.needRegisterAccepter = true;
    }

    public static void runRegisterAccepterTasks(Scheduler currentScheduler) {
        RegisterAccepterTask[] tasks = registerAccepterTasks;
        for (int i = 0; i < tasks.length; i++) {
            RegisterAccepterTask task = tasks[i];
            if (task != null && task.needRegisterAccepter && task.currentScheduler == currentScheduler)
                task.run(currentScheduler);
        }
    }

    public static void accept(SelectionKey key, Scheduler currentScheduler) {
        RegisterAccepterTask task = (RegisterAccepterTask) key.attachment();
        task.asyncServer.getProtocolServer().accept(currentScheduler);
        if (task.asyncServer.isRoundRobinAcceptEnabled()) {
            Scheduler scheduler = SchedulerFactory.getScheduler();
            // 如果下一个负责处理网络accept事件的调度器又是当前调度器，那么不需要做什么
            if (scheduler != currentScheduler) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_ACCEPT);
                scheduler.registerAccepter(task.asyncServer, task.serverChannel);
            }
        }
    }
}

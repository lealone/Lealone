/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DataBufferFactory;
import org.lealone.db.scheduler.SchedulerBase;
import org.lealone.server.ProtocolServer;

public abstract class NetScheduler extends SchedulerBase {

    protected final NetEventLoop netEventLoop;

    public NetScheduler(int id, String name, int schedulerCount, Map<String, String> config,
            boolean isThreadSafe) {
        super(id, name, schedulerCount, config);
        netEventLoop = NetFactoryManager.getFactory(config).createNetEventLoop(loopInterval,
                isThreadSafe);
        netEventLoop.setOwner(this);
        netEventLoop.setScheduler(this);
    }

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
    public void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel) {
        DbException.throwInternalError();
    }

    // --------------------- 网络事件循环 ---------------------

    @Override
    public void wakeUp() {
        netEventLoop.wakeup();
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
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server;

import com.lealone.db.DataBuffer;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.NetBuffer;
import com.lealone.net.TransferConnection;
import com.lealone.net.WritableChannel;
import com.lealone.server.scheduler.ServerSessionInfo;

public abstract class AsyncServerConnection extends TransferConnection {

    protected final InternalScheduler scheduler;
    protected final NetBuffer inNetBuffer;

    public AsyncServerConnection(WritableChannel writableChannel, Scheduler scheduler) {
        super(writableChannel, true);
        this.scheduler = (InternalScheduler) scheduler;
        DataBuffer dataBuffer = scheduler.getDataBufferFactory().create(NetBuffer.BUFFER_SIZE);
        inNetBuffer = new NetBuffer(dataBuffer, false);
        inNetBuffer.setGlobal(true);
    }

    @Override
    public NetBuffer getNetBuffer() {
        return inNetBuffer;
    }

    public abstract void closeSession(ServerSessionInfo si);

    public abstract int getSessionCount();

}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.lealone.common.logging.Logger;
import org.lealone.storage.page.PageOperation.PageOperationResult;

public abstract class PageOperationHandlerBase extends Thread implements PageOperationHandler {

    // LinkedBlockingQueue测出的性能不如ConcurrentLinkedQueue好
    protected final ConcurrentLinkedQueue<PageOperation> pageOperations = new ConcurrentLinkedQueue<>();
    protected final AtomicLong size = new AtomicLong();
    protected final int handlerId;
    protected final AtomicReferenceArray<PageOperationHandler> waitingHandlers;
    protected final AtomicBoolean hasWaitingHandlers = new AtomicBoolean(false);

    public PageOperationHandlerBase(int handlerId, String name, int waitingQueueSize) {
        super(name);
        setDaemon(false);
        this.handlerId = handlerId;
        waitingHandlers = new AtomicReferenceArray<>(waitingQueueSize);
    }

    protected abstract Logger getLogger();

    @Override
    public int getHandlerId() {
        return handlerId;
    }

    @Override
    public long getLoad() {
        return size.get();
    }

    @Override
    public void handlePageOperation(PageOperation po) {
        size.incrementAndGet();
        pageOperations.add(po);
        wakeUp();
    }

    @Override
    public void addWaitingHandler(PageOperationHandler handler) {
        int id = handler.getHandlerId();
        if (id >= 0) {
            waitingHandlers.set(id, handler);
            hasWaitingHandlers.set(true);
        }
    }

    @Override
    public void wakeUpWaitingHandlers() {
        if (hasWaitingHandlers.compareAndSet(true, false)) {
            for (int i = 0, length = waitingHandlers.length(); i < length; i++) {
                PageOperationHandler handler = waitingHandlers.get(i);
                if (handler != null) {
                    handler.wakeUp();
                    waitingHandlers.compareAndSet(i, handler, null);
                }
            }
        }
    }

    protected void runPageOperationTasks() {
        if (size.get() <= 0)
            return;
        long size = this.size.get();
        for (int i = 0; i < size; i++) {
            PageOperation po = pageOperations.poll();
            while (true) {
                try {
                    PageOperationResult result = po.run(this);
                    if (result == PageOperationResult.LOCKED) {
                        pageOperations.add(po);
                    } else if (result == PageOperationResult.RETRY) {
                        continue;
                    } else {
                        this.size.decrementAndGet();
                    }
                } catch (Throwable e) {
                    this.size.decrementAndGet();
                    getLogger().warn("Failed to run page operation: " + po, e);
                }
                break;
            }
        }
    }
}

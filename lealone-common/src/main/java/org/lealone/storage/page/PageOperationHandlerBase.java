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
    protected PageOperation lockedTask;

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
    public void handlePageOperation(PageOperation task) {
        size.incrementAndGet();
        pageOperations.add(task);
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
        PageOperation task;
        // 先执行上一个被锁定的task，严格保证每个PageOperation的执行顺序
        if (lockedTask != null) {
            task = lockedTask;
            lockedTask = null;
        } else {
            task = pageOperations.poll();
        }
        while (task != null) {
            try {
                PageOperationResult result = task.run(this);
                if (result == PageOperationResult.LOCKED) {
                    lockedTask = task;
                    break;
                } else if (result == PageOperationResult.RETRY) {
                    continue;
                }
            } catch (Throwable e) {
                getLogger().warn("Failed to run page operation: " + task, e);
            }
            size.decrementAndGet();
            task = pageOperations.poll();
        }
    }
}

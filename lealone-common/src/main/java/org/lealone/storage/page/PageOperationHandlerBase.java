/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.logging.Logger;
import org.lealone.storage.page.PageOperation.PageOperationResult;

public abstract class PageOperationHandlerBase extends Thread implements PageOperationHandler {

    // LinkedBlockingQueue测出的性能不如ConcurrentLinkedQueue好
    protected final ConcurrentLinkedQueue<PageOperation> pageOperations = new ConcurrentLinkedQueue<>();
    protected final ConcurrentLinkedQueue<PageOperationHandler> waitingHandlers = new ConcurrentLinkedQueue<>();
    protected final AtomicLong size = new AtomicLong();

    public PageOperationHandlerBase(String name) {
        super(name);
        setDaemon(true);
    }

    protected abstract Logger getLogger();

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
        waitingHandlers.add(handler);
    }

    @Override
    public void wakeUpWaitingHandlers() {
        if (!waitingHandlers.isEmpty()) {
            for (PageOperationHandler handler : waitingHandlers) {
                handler.wakeUp();
            }
            waitingHandlers.clear();
        }
    }

    protected void runPageOperationTasks() {
        // 先peek，执行成功时再poll，严格保证每个PageOperation的执行顺序
        PageOperation task = pageOperations.peek();
        while (task != null) {
            try {
                PageOperationResult result = task.run(this);
                if (result == PageOperationResult.LOCKED) {
                    break;
                } else if (result == PageOperationResult.RETRY) {
                    continue;
                }
            } catch (Throwable e) {
                getLogger().warn("Failed to run page operation: " + task, e);
            }
            pageOperations.poll();
            size.decrementAndGet();
            task = pageOperations.peek();
        }
    }
}

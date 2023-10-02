/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.lealone.common.logging.Logger;
import org.lealone.db.link.LinkableBase;
import org.lealone.db.link.LinkableList;
import org.lealone.db.session.Session;
import org.lealone.storage.page.PageOperation.PageOperationResult;

public abstract class PageOperationHandlerBase extends Thread implements PageOperationHandler {

    private static class LinkablePageOperation extends LinkableBase<LinkablePageOperation> {
        final PageOperation po;

        public LinkablePageOperation(PageOperation po) {
            this.po = po;
        }
    }

    protected final LinkableList<LinkablePageOperation> lockedTasks = new LinkableList<>();

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
        return lockedTasks.size();
    }

    @Override
    public void handlePageOperation(PageOperation po) {
        lockedTasks.add(new LinkablePageOperation(po));
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
        if (lockedTasks.isEmpty())
            return;
        while (lockedTasks.getHead() != null) {
            int size = lockedTasks.size();
            LinkablePageOperation task = lockedTasks.getHead();
            LinkablePageOperation last = null;
            while (task != null) {
                Session old = getCurrentSession();
                setCurrentSession(task.po.getSession());
                try {
                    PageOperationResult result = task.po.run(this);
                    if (result == PageOperationResult.LOCKED) {
                        last = task;
                        task = task.next;
                        continue;
                    } else if (result == PageOperationResult.RETRY) {
                        continue;
                    }
                    task = task.next;
                    if (last == null)
                        lockedTasks.setHead(task);
                    else
                        last.next = task;
                } catch (Throwable e) {
                    getLogger().warn("Failed to run page operation: " + task, e);
                } finally {
                    setCurrentSession(old);
                }
                lockedTasks.decrementSize();
            }
            if (lockedTasks.getHead() == null)
                lockedTasks.setTail(null);
            else
                lockedTasks.setTail(last);

            // 全都锁住了，没必要再试了
            if (size == lockedTasks.size())
                break;
        }
    }
}

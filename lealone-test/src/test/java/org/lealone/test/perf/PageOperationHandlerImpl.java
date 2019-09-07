/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.perf;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.util.DateTimeUtils;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.storage.PageOperationHandlerFactory;
import org.lealone.storage.aose.btree.BTreeMap;

public class PageOperationHandlerImpl extends Thread implements PageOperationHandler {

    private static int pageOperationHandlerCount = 0; // Runtime.getRuntime().availableProcessors();
    private static PageOperationHandlerImpl[] pageOperationHandlers = new PageOperationHandlerImpl[pageOperationHandlerCount];

    private static PageOperationHandlerImpl nodePageHandler;

    public static void setPageOperationHandlersCount(int count) {
        pageOperationHandlerCount = count;
        // pageOperationHandlersCount--;
        pageOperationHandlers = new PageOperationHandlerImpl[pageOperationHandlerCount];
    }

    public static int getPageOperationHandlerCount() {
        return pageOperationHandlerCount;
    }

    public static synchronized void startNodePageOperationHandler(Map<String, String> config) {
        if (nodePageHandler != null)
            return;
        nodePageHandler = new PageOperationHandlerImpl(10000, config);
        nodePageHandler.setDaemon(true);
        nodePageHandler.start();
    }

    public static synchronized void pauseAll() {
        nodePageHandler.pause2();
        for (int i = 0; i < pageOperationHandlerCount; i++) {
            pageOperationHandlers[i].pause2();
        }
    }

    public static synchronized void resumeAll() {
        nodePageHandler.resume2();
        for (int i = 0; i < pageOperationHandlerCount; i++) {
            pageOperationHandlers[i].resume2();
        }
    }

    public static PageOperationHandlerImpl getNodePageOperationHandler() {
        return nodePageHandler;
    }

    public static synchronized void startPageOperationHandlers(Map<String, String> config) {
        // if (rootHandler != null)
        // return;
        // rootHandler = new PageOperationHandler(10000, config);
        // rootHandler = new PageOperationHandler(10000);
        for (int i = 0; i < pageOperationHandlerCount; i++) {
            pageOperationHandlers[i] = new PageOperationHandlerImpl(i, config);
            pageOperationHandlers[i].setDaemon(true);
        }

        PageOperationHandlerFactory.setPageOperationHandlers(pageOperationHandlers);

        for (int i = 0; i < pageOperationHandlerCount; i++) {
            pageOperationHandlers[i].start();
        }
        // rootHandler.setDaemon(true);
        // rootHandler.start();
    }

    private static final AtomicInteger index = new AtomicInteger(0);

    static synchronized void stopPageOperationHandlers() {
        // if (nodePageHandler == null)
        // return;

        for (int i = 0; i < pageOperationHandlerCount; i++) {
            pageOperationHandlers[i].end();
        }
        nodePageHandler.end();

        for (int i = 0; i < pageOperationHandlerCount; i++) {
            try {
                pageOperationHandlers[i].join();
            } catch (InterruptedException e) {
            }
        }
        try {
            nodePageHandler.join();
        } catch (InterruptedException e) {
        }
        nodePageHandler = null;
    }

    public static PageOperationHandler getNextHandler() {
        if (pageOperationHandlerCount <= 0)
            return nodePageHandler;
        return pageOperationHandlers[index.getAndIncrement() % pageOperationHandlers.length];
    }

    public static PageOperationHandler getNextHandler(int id) {
        if (pageOperationHandlerCount <= 0)
            return nodePageHandler;
        return pageOperationHandlers[id % pageOperationHandlers.length];
    }

    public static PageOperationHandler getHandler(long key) {
        if (pageOperationHandlerCount <= 0)
            return nodePageHandler;
        return pageOperationHandlers[(int) (key % pageOperationHandlers.length)];
    }

    public static PageOperationHandler getHandler(int id) {
        if (pageOperationHandlerCount <= 0)
            return nodePageHandler;
        return pageOperationHandlers[id];
    }

    private final int id;
    private ConcurrentLinkedQueue<PageOperation> tasks = new ConcurrentLinkedQueue<>();
    private final Semaphore haveWork = new Semaphore(1);
    private final long loopInterval;
    private boolean stop;
    private boolean pause;

    private final ConcurrentSkipListMap<Long, ConcurrentLinkedQueue<PageOperation>> normalPageQueues = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<Long, ConcurrentLinkedQueue<PageOperation>> splittingPageQueues = new ConcurrentSkipListMap<>();

    private PageOperationHandlerImpl(int id) {
        super("PageOperationHandler-" + id);
        this.id = id;
        loopInterval = 0;
    }

    private PageOperationHandlerImpl(int id, Map<String, String> config) {
        super("PageOperationHandler-" + id);
        this.id = id;
        // setDaemon(true);
        // 默认100毫秒
        loopInterval = DateTimeUtils.getLoopInterval(config, "command_handler_loop_interval", 100);

        ShutdownHookUtils.addShutdownHook(this, () -> {
            System.out.println(PageOperationHandlerImpl.this.getName() + " total: " + totalCount.get() + " size: "
                    + size.get() + " sleep: " + sleep + " split: " + BTreeMap.splitCount.get() + " put: "
                    + BTreeMap.putCount.get() + " addUpdateCounterTaskCount: "
                    + BTreeMap.addUpdateCounterTaskCount.get() + " runUpdateCounterTaskCount: "
                    + BTreeMap.runUpdateCounterTaskCount.get());

            // for (Object[] a : PageOperation.list1) {
            // // System.out.println(a[0] + " " + a[1]);
            // }
        });
    }

    public void addQueue(long pageId, ConcurrentLinkedQueue<PageOperation> queue) {
        normalPageQueues.put(pageId, queue);
    }

    public void addSplittingPageQueue(long pageId, ConcurrentLinkedQueue<PageOperation> queue) {
        splittingPageQueues.put(pageId, queue);
    }

    public void removeQueue(long pageId) {
        normalPageQueues.remove(pageId);
    }

    public int getHandlerId() {
        return id;
    }

    public ConcurrentLinkedQueue<PageOperation> getTasks() {
        return tasks;
    }

    public ConcurrentLinkedQueue<PageOperation> getClearTasks() {
        ConcurrentLinkedQueue<PageOperation> tasks = this.tasks;
        this.tasks = new ConcurrentLinkedQueue<>();
        return tasks;
    }

    long sleep = 0;
    AtomicLong size = new AtomicLong();
    AtomicLong totalCount = new AtomicLong();

    @Override
    public void handlePageOperation(PageOperation task) {
        size.incrementAndGet();
        totalCount.incrementAndGet();
        tasks.add(task);
        // haveWorkAll.release(pageOperationHandlerCount);
        wakeUp();
    }

    ArrayList<Long> list = new ArrayList<>();

    // private void executeTasks() {
    // PageOperation task = tasks.poll();
    // // long t1 = System.currentTimeMillis();
    // while (task != null) {
    // task.run(this);
    // task = tasks.poll();
    // }
    // }

    // private void executeTasks() {
    // for (ConcurrentLinkedQueue<PageOperation> tasks : taskQueues.values()) {
    // PageOperation task = tasks.poll();
    // // long t1 = System.currentTimeMillis();
    // while (task != null) {
    // size++;
    // // size--;
    // PageOperation next = task.run(this);
    // while (next != null) {
    // next = next.run(this);
    // size++;
    // }
    // task = tasks.poll();
    // // if (System.currentTimeMillis() - t1 > 50) {
    // // System.out.println(Thread.currentThread() + " " + size);
    // // list.add(size);
    // // }
    // }
    // }
    // // for (Long size : list) {
    // // System.out.println(Thread.currentThread() + " " + size);
    // // }
    // }

    public void executeTasks() {
        // for (Entry<Long, ConcurrentLinkedQueue<PageOperation>> e : normalPageQueues.entrySet()) {
        // Long pageId = e.getKey();
        // ConcurrentLinkedQueue<PageOperation> queue = e.getValue();
        // PageOperation task = queue.poll();
        // while (task != null) {
        // PageOperationResult result = task.run(this);
        // if (result == PageOperationResult.SPLITTING) {
        // splittingPageQueues.put(pageId, queue);
        // normalPageQueues.remove(pageId);
        // break;
        // }
        // task = queue.poll();
        // }
        // }

        PageOperation task = tasks.poll();
        while (task != null) {
            size.decrementAndGet();
            task.run(this);
            task = tasks.poll();
        }

        // task = alltasks.poll();
        // while (task != null) {
        // task.run(this);
        // size.decrementAndGet();
        // task = alltasks.poll();
        // }
    }

    // private void executeTasks() {
    // PageOperation task = allTasks.poll();
    // // long t1 = System.currentTimeMillis();
    // while (task != null) {
    // task.run(this);
    // task = allTasks.poll();
    // }
    // }

    @Override
    public void run() {
        // SQLEngineManager.getInstance().setSQLStatementExecutor(this);
        while (!stop) {
            if (pause) {
                synchronized (this) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        throw new AssertionError();
                    }
                }
            }
            executeTasks();
            try {
                haveWork.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
                // haveWork.drainPermits();
                sleep++;
            } catch (InterruptedException e) {
                throw new AssertionError();
            }
        }
    }

    private void end() {
        stop = true;
        wakeUp();
    }

    private void pause2() {
        pause = true;
        haveWork.release(1);
    }

    private synchronized void resume2() {
        pause = false;
        this.notify();
        haveWork.release(1);
    }

    public void wakeUp() {
        haveWork.release(1);
        pause = false;
    }

    @Override
    public String toString() {
        return "PageOperationHandler [id=" + id + ", size=" + size + "]";
    }

    // public static ConcurrentLinkedQueue<PageOperation> alltasks = new ConcurrentLinkedQueue<>();
    // private static final Semaphore haveWorkAll = new Semaphore(1);

    public static void addPageOperation(PageOperation task) {
        getNextHandler().handlePageOperation(task);
        // alltasks.add(task);
        // haveWorkAll.release(pageOperationHandlerCount);
    }

}

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
package org.lealone.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.DateTimeUtils;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.db.async.AsyncResult;

public class DefaultPageOperationHandler implements PageOperationHandler, Runnable, PageOperation.Listener<Object> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPageOperationHandler.class);
    // LinkedBlockingQueue测出的性能不如ConcurrentLinkedQueue好
    private final ConcurrentLinkedQueue<PageOperation> tasks = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<PageOperation> tasks2 = new ConcurrentLinkedQueue<>();
    private final AtomicLong size = new AtomicLong();
    private final Semaphore haveWork = new Semaphore(1);
    private final String name;
    private final long loopInterval;
    private Thread thread;
    private boolean stopped;
    private long shiftCount;

    public DefaultPageOperationHandler(int id, Map<String, String> config) {
        this(DefaultPageOperationHandler.class.getSimpleName() + "-" + id, config);
    }

    public DefaultPageOperationHandler(String name, Map<String, String> config) {
        this.name = name;
        // 默认100毫秒
        loopInterval = DateTimeUtils.getLoopInterval(config, "page_operation_handler_loop_interval", 100);
    }

    @Override
    public long getLoad() {
        return size.get();
    }

    @Override
    public void handlePageOperation(PageOperation task) {
        size.incrementAndGet();
        tasks.add(task);
        wakeUp();
    }

    @Override
    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }

    public void reset(boolean clearTasks) {
        thread = null;
        stopped = false;
        shiftCount = 0;
        if (clearTasks) {
            size.set(0);
            tasks.clear();
        }
    }

    public void start() {
        if (thread != null)
            return;
        stopped = false;
        ShutdownHookUtils.addShutdownHook(name, () -> {
            stop();
        });
        thread = new Thread(this, name);
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() {
        stopped = true;
        thread = null;
        wakeUp();
    }

    public void wakeUp() {
        haveWork.release(1);
    }

    public long getShiftCount() {
        return shiftCount;
    }

    public ConcurrentLinkedQueue<PageOperation> tasks2() {
        return tasks2;
    }

    @Override
    public void run() {
        while (!stopped) {
            runTasks();
            try {
                haveWork.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
                haveWork.drainPermits();
            } catch (InterruptedException e) {
                stopped = true;
                // logger.warn(getName() + " is interrupted");
                break;
            }
        }
    }

    private void runTasks() {
        PageOperation task = tasks.poll();
        while (task != null) {
            size.decrementAndGet();
            // tasks2.add(task);
            try {
                task.run(this);
                // PageOperationResult result = task.run(this);
                // if (result == PageOperationResult.SHIFTED) {
                // shiftCount++;
                // }
            } catch (Throwable e) {
                logger.warn("Failed to run page operation: " + task, e);
            }
            task = tasks.poll();
        }
    }

    // 以下使用同步方式执行
    private volatile RuntimeException e;
    private volatile Object result;

    @Override
    public Object await() {
        e = null;
        result = null;
        while (result == null || e == null) {
            runTasks();
            try {
                haveWork.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
                haveWork.drainPermits();
            } catch (InterruptedException e) {
                break;
            }
        }
        if (e != null)
            throw e;
        return result;
    }

    @Override
    public void handle(AsyncResult<Object> ar) {
        if (ar.isSucceeded())
            result = ar.getResult();
        else
            e = new RuntimeException(ar.getCause());
        wakeUp();
    }
}

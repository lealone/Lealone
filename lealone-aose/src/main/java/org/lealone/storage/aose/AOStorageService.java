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
package org.lealone.storage.aose;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.lealone.common.concurrent.DebuggableThreadPoolExecutor;
import org.lealone.common.util.DateTimeUtils;

public class AOStorageService extends Thread {

    private static final CopyOnWriteArrayList<Callable<?>> pendingTasks = new CopyOnWriteArrayList<>();

    private static final ExecutorService executorService = new DebuggableThreadPoolExecutor("AOStorageServiceThread", 1,
            Runtime.getRuntime().availableProcessors(), 6000, TimeUnit.MILLISECONDS);

    private static final AOStorageService INSTANCE = new AOStorageService();

    public static AOStorageService getInstance() {
        return INSTANCE;
    }

    public static Future<?> submitTask(Callable<?> task) {
        return executorService.submit(task);
    }

    public static void submitTask(Runnable task) {
        executorService.submit(task);
    }

    public static void addPendingTask(Callable<?> task) {
        pendingTasks.add(task);
    }

    private long loopInterval;
    private boolean running;

    private AOStorageService() {
        super("AOStorageService");
        setDaemon(true);
    }

    void close() {
        running = false;
    }

    public synchronized void start(Map<String, String> config) {
        // 默认3秒钟
        loopInterval = DateTimeUtils.getLoopInterval(config, "storage_service_loop_interval", 3000);
        start();
    }

    @Override
    public synchronized void start() {
        if (!running) {
            running = true;
            super.start();
        }
    }

    @Override
    public void run() {
        while (running) {
            try {
                sleep(loopInterval);
            } catch (InterruptedException e) {
                continue;
            }

            merge();
        }
    }

    private static void merge() {
        synchronized (pendingTasks) {
            ArrayList<Future<?>> futures = new ArrayList<>(pendingTasks.size());
            ArrayList<Callable<?>> list = new ArrayList<>(pendingTasks);
            for (Callable<?> task : list) {
                Future<?> f = submitTask(task);
                futures.add(f);
                pendingTasks.remove(task);
            }
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }
}

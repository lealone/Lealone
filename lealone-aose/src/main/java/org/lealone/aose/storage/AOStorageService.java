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
package org.lealone.aose.storage;

import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.lealone.aose.concurrent.DebuggableThreadPoolExecutor;
import org.lealone.aose.util.Utils;

public class AOStorageService extends Thread {

    private static final CopyOnWriteArrayList<BufferedMap<?, ?>> bufferedMaps = new CopyOnWriteArrayList<>();
    private static final CopyOnWriteArrayList<AOMap<?, ?>> aoMaps = new CopyOnWriteArrayList<>();

    private static final ExecutorService executorService = new DebuggableThreadPoolExecutor("AOStorageServiceThread", 0,
            Utils.getAvailableProcessors(), 6000, TimeUnit.MILLISECONDS);

    private static final LinkedBlockingQueue<Callable<?>> taskQueue = new LinkedBlockingQueue<>();

    private static final AOStorageService INSTANCE = new AOStorageService();

    static AOStorageService getInstance() {
        return INSTANCE;
    }

    public static void addTask(Callable<?> task) {
        taskQueue.add(task);
    }

    public static void addBufferedMap(BufferedMap<?, ?> map) {
        bufferedMaps.add(map);
    }

    public static void removeBufferedMap(BufferedMap<?, ?> map) {
        bufferedMaps.remove(map);
    }

    public static void addAOMap(AOMap<?, ?> map) {
        aoMaps.add(map);
    }

    public static void removeAOMap(AOMap<?, ?> map) {
        aoMaps.remove(map);
    }

    private final int sleep;
    private boolean running;

    private AOStorageService() {
        super("AOStorageService");
        this.sleep = 3000;
        setDaemon(true);
    }

    void close() {
        running = false;
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
        long lastTime = System.currentTimeMillis();
        while (running) {
            try {
                Callable<?> task = taskQueue.poll(sleep, TimeUnit.MILLISECONDS);
                if (task != null) {
                    executorService.submit(task);
                    continue;
                }

                if (System.currentTimeMillis() - lastTime > sleep) {
                    AOStorageService.addTask(mergeTask);
                    lastTime = System.currentTimeMillis();
                }
            } catch (InterruptedException e) {
                continue;
            }
        }
    }

    private static final Callable<?> mergeTask = () -> {
        adaptiveOptimization();
        merge();
        return null;
    };

    private static void adaptiveOptimization() {
        for (AOMap<?, ?> map : aoMaps) {
            if (map.getReadPercent() > 50)
                map.switchToNoBufferedMap();
            else if (map.getWritePercent() > 50)
                map.switchToBufferedMap();
        }
    }

    private static void merge() {
        for (BufferedMap<?, ?> map : bufferedMaps) {
            executorService.submit(map);
        }
    }
}

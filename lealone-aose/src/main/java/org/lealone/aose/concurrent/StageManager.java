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
package org.lealone.aose.concurrent;

import java.util.EnumMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.lealone.aose.util.Utils;
import org.lealone.common.concurrent.LealoneExecutorService;
import org.lealone.common.concurrent.NamedThreadFactory;

/**
 * This class manages executor services for Messages recieved: each Message requests
 * running on a specific "stage" for concurrency control; hence the Map approach,
 * even though stages (executors) are not created dynamically.
 */
public class StageManager {
    private static final EnumMap<Stage, LealoneExecutorService> stages = new EnumMap<>(Stage.class);

    private static final long KEEPALIVE = 60; // seconds to keep "extra" threads alive for when idle

    static {
        stages.put(Stage.REQUEST_RESPONSE,
                multiThreadedLowSignalStage(Stage.REQUEST_RESPONSE, Utils.getAvailableProcessors()));
        stages.put(Stage.INTERNAL_RESPONSE,
                multiThreadedStage(Stage.INTERNAL_RESPONSE, Utils.getAvailableProcessors()));
        // the rest are all single-threaded
        stages.put(Stage.GOSSIP, new MetricsEnabledThreadPoolExecutor(Stage.GOSSIP));
    }

    private static MetricsEnabledThreadPoolExecutor multiThreadedStage(Stage stage, int numThreads) {
        return new MetricsEnabledThreadPoolExecutor(numThreads, KEEPALIVE, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(stage.getJmxName()), stage.getJmxType());
    }

    private static LealoneExecutorService multiThreadedLowSignalStage(Stage stage, int numThreads) {
        return MetricsEnabledSharedExecutorPool.SHARED.newExecutor(numThreads, Integer.MAX_VALUE, stage.getJmxName(),
                stage.getJmxType());
    }

    /**
     * Retrieve a stage from the StageManager
     * @param stage name of the stage to be retrieved.
     */
    public static LealoneExecutorService getStage(Stage stage) {
        return stages.get(stage);
    }

    /**
     * This method shuts down all registered stages.
     */
    public static void shutdownNow() {
        for (Stage stage : Stage.values()) {
            StageManager.stages.get(stage).shutdownNow();
        }
    }
}

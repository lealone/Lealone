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
package org.lealone.cluster.concurrent;

import static org.lealone.cluster.config.DatabaseDescriptor.getConcurrentReaders;
import static org.lealone.cluster.config.DatabaseDescriptor.getConcurrentWriters;

import java.util.EnumMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.lealone.cluster.net.MessagingService;
import org.lealone.cluster.tracing.TraceState;
import org.lealone.cluster.utils.FBUtilities;

/**
 * This class manages executor services for Messages recieved: each Message requests
 * running on a specific "stage" for concurrency control; hence the Map approach,
 * even though stages (executors) are not created dynamically.
 */
public class StageManager {
    private static final EnumMap<Stage, TracingAwareExecutorService> stages = new EnumMap<Stage, TracingAwareExecutorService>(
            Stage.class);

    public static final long KEEPALIVE = 60; // seconds to keep "extra" threads alive for when idle

    static {
        stages.put(Stage.MUTATION, multiThreadedLowSignalStage(Stage.MUTATION, getConcurrentWriters()));
        //stages.put(Stage.COUNTER_MUTATION, multiThreadedLowSignalStage(Stage.COUNTER_MUTATION, getConcurrentCounterWriters()));
        stages.put(Stage.READ, multiThreadedLowSignalStage(Stage.READ, getConcurrentReaders()));
        stages.put(Stage.REQUEST_RESPONSE,
                multiThreadedLowSignalStage(Stage.REQUEST_RESPONSE, FBUtilities.getAvailableProcessors()));
        stages.put(Stage.INTERNAL_RESPONSE, multiThreadedStage(Stage.INTERNAL_RESPONSE, FBUtilities.getAvailableProcessors()));
        // the rest are all single-threaded
        stages.put(Stage.GOSSIP, new MetricsEnabledThreadPoolExecutor(Stage.GOSSIP));
        //stages.put(Stage.ANTI_ENTROPY, new MetricsEnabledThreadPoolExecutor(Stage.ANTI_ENTROPY));
        stages.put(Stage.MIGRATION, new MetricsEnabledThreadPoolExecutor(Stage.MIGRATION));
        stages.put(Stage.MISC, new MetricsEnabledThreadPoolExecutor(Stage.MISC));
        //stages.put(Stage.READ_REPAIR, multiThreadedStage(Stage.READ_REPAIR, FBUtilities.getAvailableProcessors()));
        stages.put(Stage.TRACING, tracingExecutor());
    }

    private static ExecuteOnlyExecutor tracingExecutor() {
        RejectedExecutionHandler reh = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                MessagingService.instance().incrementDroppedMessages(MessagingService.Verb._TRACE);
            }
        };
        return new ExecuteOnlyExecutor(1, 1, KEEPALIVE, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000),
                new NamedThreadFactory(Stage.TRACING.getJmxName()), reh);
    }

    private static MetricsEnabledThreadPoolExecutor multiThreadedStage(Stage stage, int numThreads) {
        return new MetricsEnabledThreadPoolExecutor(numThreads, KEEPALIVE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory(stage.getJmxName()), stage.getJmxType());
    }

    private static TracingAwareExecutorService multiThreadedLowSignalStage(Stage stage, int numThreads) {
        return MetricsEnabledSharedExecutorPool.SHARED.newExecutor(numThreads, Integer.MAX_VALUE, stage.getJmxName(),
                stage.getJmxType());
    }

    /**
     * Retrieve a stage from the StageManager
     * @param stage name of the stage to be retrieved.
     */
    public static TracingAwareExecutorService getStage(Stage stage) {
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

    /**
     * A TPE that disallows submit so that we don't need to worry about unwrapping exceptions on the
     * tracing stage.  See lealone-1123 for background.
     */
    private static class ExecuteOnlyExecutor extends ThreadPoolExecutor implements TracingAwareExecutorService {
        public ExecuteOnlyExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        }

        @Override
        public void execute(Runnable command, TraceState state) {
            assert state == null;
            super.execute(command);
        }

        @Override
        public void maybeExecuteImmediately(Runnable command) {
            execute(command);
        }

        @Override
        public Future<?> submit(Runnable task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            throw new UnsupportedOperationException();
        }
    }
}

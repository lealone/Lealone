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
package org.lealone.replication;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class ThreadPool {
    public final static ThreadPoolExecutor executor = getThreadPoolExecutor();

    private static ThreadPoolExecutor getThreadPoolExecutor() {
        int corePoolSize = 3;
        int maxPoolSize = Integer.MAX_VALUE;
        int keepAliveTime = 3;

        ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), new NamedThreadFactory(ThreadPool.class.getSimpleName()));
        executor.allowCoreThreadTimeOut(true);

        return executor;
    }

    private static class NamedThreadFactory implements ThreadFactory {
        protected final String id;
        private final int priority;
        protected final AtomicInteger n = new AtomicInteger(1);

        public NamedThreadFactory(String id) {
            this(id, Thread.NORM_PRIORITY);
        }

        public NamedThreadFactory(String id, int priority) {
            this.id = id;
            this.priority = priority;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            String name = id + ":" + n.getAndIncrement();
            Thread thread = new Thread(runnable, name);
            thread.setPriority(priority);
            thread.setDaemon(true);
            return thread;
        }
    }

}

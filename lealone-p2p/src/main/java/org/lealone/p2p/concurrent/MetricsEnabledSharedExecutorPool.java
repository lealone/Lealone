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
package org.lealone.p2p.concurrent;

import org.lealone.common.concurrent.LealoneExecutorService;
import org.lealone.common.concurrent.SEPExecutor;
import org.lealone.common.concurrent.SharedExecutorPool;
import org.lealone.p2p.metrics.SEPMetrics;

public class MetricsEnabledSharedExecutorPool extends SharedExecutorPool {

    public static final MetricsEnabledSharedExecutorPool SHARED = new MetricsEnabledSharedExecutorPool("SharedPool");

    public MetricsEnabledSharedExecutorPool(String poolName) {
        super(poolName);
    }

    public class MetricsEnabledSEPExecutor extends SEPExecutor {

        private final SEPMetrics metrics;

        public MetricsEnabledSEPExecutor(int poolSize, int maxQueuedLength, String name, String jmxPath) {
            super(MetricsEnabledSharedExecutorPool.this, poolSize, maxQueuedLength);
            metrics = new SEPMetrics(this, jmxPath, name);
        }

        private void unregisterMBean() {
            // release metrics
            metrics.release();
        }

        @Override
        public synchronized void shutdown() {
            // synchronized, because there is no way to access super.mainLock, which would be
            // the preferred way to make this threadsafe
            if (!isShutdown()) {
                unregisterMBean();
            }
            super.shutdown();
        }
    }

    public LealoneExecutorService newExecutor(int maxConcurrency, int maxQueuedTasks, String name, String jmxPath) {
        MetricsEnabledSEPExecutor executor = new MetricsEnabledSEPExecutor(maxConcurrency, maxQueuedTasks, name,
                jmxPath);
        executors.add(executor);
        return executor;
    }
}

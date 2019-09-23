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
package org.lealone.db.async;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncTaskHandlerFactory {

    private static AsyncTaskHandler DEFAULT_HANDLER = new AsyncTaskHandler() {
        @Override
        public void handle(AsyncTask task) {
            task.run();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(AsyncTask task, long initialDelay, long delay, TimeUnit unit) {
            task.run();
            return null;
        }
    };

    private static final AtomicInteger index = new AtomicInteger(0);
    private static AsyncTaskHandler[] handlers = { DEFAULT_HANDLER };

    public static void setAsyncTaskHandlers(AsyncTaskHandler[] handlers) {
        AsyncTaskHandlerFactory.handlers = handlers;
    }

    public static AsyncTaskHandler getAsyncTaskHandler() {
        return handlers[index.getAndIncrement() % handlers.length];
    }
}

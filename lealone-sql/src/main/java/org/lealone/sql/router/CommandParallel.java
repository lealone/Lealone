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
package org.lealone.sql.router;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.New;
import org.lealone.db.result.Result;
import org.lealone.sql.dml.Select;

public class CommandParallel {
    private final static ThreadPoolExecutor pool = initPool();

    static class NamedThreadFactory implements ThreadFactory {
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

    private static ThreadPoolExecutor initPool() {
        // int corePoolSize = HBaseUtils.getConfiguration().getInt(COMMAND_PARALLEL_CORE_POOL_SIZE,
        // DEFAULT_COMMAND_PARALLEL_CORE_POOL_SIZE);
        // int maxPoolSize = HBaseUtils.getConfiguration().getInt(COMMAND_PARALLEL_MAX_POOL_SIZE,
        // DEFAULT_COMMAND_PARALLEL_MAX_POOL_SIZE);
        // int keepAliveTime = HBaseUtils.getConfiguration().getInt(COMMAND_PARALLEL_KEEP_ALIVE_TIME,
        // DEFAULT_COMMAND_PARALLEL_KEEP_ALIVE_TIME);

        int corePoolSize = 3;
        int maxPoolSize = Integer.MAX_VALUE;
        int keepAliveTime = 3;

        ThreadPoolExecutor pool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), new NamedThreadFactory(CommandParallel.class.getSimpleName()));
        pool.allowCoreThreadTimeOut(true);

        return pool;
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        return pool;
    }

    public static String getPlanSQL(Select select) {
        if (select.isGroupQuery() || select.getLimit() != null)
            return select.getPlanSQL(true);
        else
            return select.getSQL();
    }

    public static int executeUpdateCallable(List<Callable<Integer>> commands) {
        int size = commands.size();
        List<Future<Integer>> futures = New.arrayList(size);
        for (int i = 0; i < size; i++) {
            futures.add(pool.submit(commands.get(i)));
        }
        int updateCount = 0;
        try {
            for (int i = 0; i < size; i++) {
                updateCount += futures.get(i).get();
            }
        } catch (Exception e) {
            throwException(e);
        }
        return updateCount;
    }

    public static List<Result> executeSelectCallable(List<Callable<Result>> commands) {
        int size = commands.size();
        List<Future<Result>> futures = New.arrayList(size);
        List<Result> results = New.arrayList(size);
        for (int i = 0; i < size; i++) {
            futures.add(pool.submit(commands.get(i)));
        }
        try {
            for (int i = 0; i < size; i++) {
                results.add(futures.get(i).get());
            }
        } catch (Exception e) {
            throwException(e);
        }
        return results;
    }

    private static void throwException(Throwable e) {
        if (e instanceof ExecutionException)
            e = ((ExecutionException) e).getCause();
        throw DbException.convert(e);
    }
}

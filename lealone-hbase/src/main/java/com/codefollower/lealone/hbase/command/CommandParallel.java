/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.codefollower.lealone.hbase.command;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Threads;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.command.CommandRemote;
import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.command.dml.Select;
import com.codefollower.lealone.hbase.command.dml.Task;
import com.codefollower.lealone.hbase.command.dml.WithWhereClause;
import com.codefollower.lealone.hbase.command.merge.HBaseMergedResult;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.result.HBaseSerializedResult;
import com.codefollower.lealone.hbase.result.HBaseSortedResult;
import com.codefollower.lealone.result.ResultInterface;
import com.codefollower.lealone.util.New;

public class CommandParallel {
    private final static ThreadPoolExecutor pool = getPool();

    private static ThreadPoolExecutor getPool() {
        //TODO 可配置的线程池 参数
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 20, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                Threads.newDaemonThreadFactory(CommandParallel.class.getSimpleName()));
        pool.allowCoreThreadTimeOut(true);

        return pool;
    }

    private static String planSQL(Prepared p) {
        if (p.isQuery()) {
            Select select = (Select) p;
            if (select.isGroupQuery())
                return select.getPlanSQL(true);
            else if (select.getSortOrder() != null && select.getOffset() != null) //分布式排序时不使用Offset
                return select.getPlanSQL(false, false);
        }

        return p.getSQL();
    }

    public static ResultInterface executeQuery(HBaseSession session, Task task, Select select, final int maxRows,
            final boolean scrollable) {
        List<CommandInterface> commands = new ArrayList<CommandInterface>();
        if (task.remoteCommands != null) {
            commands.addAll(task.remoteCommands);
            for (CommandRemote c : task.remoteCommands) {
                c.setFetchSize(select.getFetchSize());
            }
        }
        if (task.localRegions != null) {
            for (String regionName : task.localRegions) {
                Prepared p = session.prepare(planSQL(select), true);
                p.setExecuteDirec(true);
                p.setFetchSize(select.getFetchSize());
                if (p instanceof WithWhereClause) {
                    ((WithWhereClause) p).getWhereClauseSupport().setRegionName(regionName);
                }
                commands.add(new CommandWrapper(p));
            }
        }
        //originalSelect.isGroupQuery()如果是false，那么按org.apache.hadoop.hbase.client.ClientScanner的功能来实现。
        //只要Select语句中出现聚合函数、groupBy、Having三者之一都被认为是GroupQuery，
        //对于GroupQuery需要把Select语句同时发给相关的RegionServer，得到结果后再合并。
        if (!select.isGroupQuery() && select.getSortOrder() == null)
            return new HBaseSerializedResult(commands, maxRows, scrollable);

        int size = commands.size();
        List<Future<ResultInterface>> futures = New.arrayList(size);
        List<ResultInterface> results = New.arrayList(size);
        for (int i = 0; i < size; i++) {
            final CommandInterface c = commands.get(i);
            futures.add(pool.submit(new Callable<ResultInterface>() {
                public ResultInterface call() throws Exception {
                    return c.executeQuery(maxRows, scrollable);
                }
            }));
        }
        try {
            for (int i = 0; i < size; i++) {
                results.add(futures.get(i).get());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!select.isGroupQuery() && select.getSortOrder() != null)
            return new HBaseSortedResult(maxRows, session, select, results);

        String newSQL = select.getPlanSQL(true);
        Select newSelect = (Select) session.prepare(newSQL, true);
        newSelect.setExecuteDirec(true);

        return new HBaseMergedResult(results, newSelect, select);
    }

    public static int executeUpdate(List<CommandInterface> commands) {
        if (commands.size() == 1) {
            CommandInterface c = commands.get(0);
            return c.executeUpdate();
        }
        int updateCount = 0;
        int size = commands.size();
        List<Future<Integer>> futures = New.arrayList(size);
        for (int i = 0; i < size; i++) {
            final CommandInterface c = commands.get(i);
            futures.add(pool.submit(new Callable<Integer>() {
                public Integer call() throws Exception {
                    return c.executeUpdate();
                }
            }));
        }
        try {
            for (int i = 0; i < size; i++) {
                updateCount += futures.get(i).get();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return updateCount;
    }

    public static int executeUpdate(Task task, Callable<Integer> call) {
        int updateCount = 0;
        List<CommandRemote> commands = task.remoteCommands;
        int size = commands.size() + 1;
        List<Future<Integer>> futures = New.arrayList(size);
        futures.add(pool.submit(call));
        for (int i = 0; i < size - 1; i++) {
            final CommandInterface c = commands.get(i);
            futures.add(pool.submit(new Callable<Integer>() {
                public Integer call() throws Exception {
                    return c.executeUpdate();
                }
            }));
        }
        try {
            for (int i = 0; i < size; i++) {
                updateCount += futures.get(i).get();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return updateCount;
    }
}

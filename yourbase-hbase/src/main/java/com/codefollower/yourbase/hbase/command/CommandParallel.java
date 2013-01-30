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
package com.codefollower.yourbase.hbase.command;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Threads;

import com.codefollower.yourbase.command.Command;
import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.command.Prepared;
import com.codefollower.yourbase.command.dml.Select;
import com.codefollower.yourbase.expression.ParameterInterface;
import com.codefollower.yourbase.hbase.command.merge.HBaseMergedResult;
import com.codefollower.yourbase.hbase.engine.HBaseSession;
import com.codefollower.yourbase.hbase.result.HBaseSerializedResult;
import com.codefollower.yourbase.hbase.util.HBaseRegionInfo;
import com.codefollower.yourbase.hbase.util.HBaseUtils;
import com.codefollower.yourbase.result.ResultInterface;
import com.codefollower.yourbase.util.New;

public class CommandParallel implements CommandInterface {
    private static ThreadPoolExecutor pool;
    private final HBaseSession originalSession;
    private final Prepared originalPrepared;
    private final String sql;
    private final List<CommandInterface> commands;
    private int fetchSize;

    public CommandParallel(HBaseSession session, CommandProxy commandProxy, //
            byte[] tableName, List<byte[]> startKeys, String sql, Prepared originalPrepared) {
        if (startKeys == null)
            throw new RuntimeException("startKeys is null");
        else if (startKeys.size() < 2)
            throw new RuntimeException("startKeys.size() < 2");

        this.originalSession = session;
        this.originalPrepared = originalPrepared;
        this.sql = sql;

        try {
            if (pool == null) {
                synchronized (CommandParallel.class) {
                    if (pool == null) {
                        //TODO 可配置的线程池参数
                        pool = new ThreadPoolExecutor(1, 20, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                                Threads.newDaemonThreadFactory(CommandParallel.class.getSimpleName()));
                        pool.allowCoreThreadTimeOut(true);
                    }
                }
            }
            commands = new ArrayList<CommandInterface>(startKeys.size());
            for (byte[] startKey : startKeys) {
                HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, startKey);
                if (CommandProxy.isLocal(originalSession, hri)) {
                    HBaseSession newSession = createHBaseSession();
                    Command c = newSession.prepareLocal(sql);
                    HBasePrepared hp = (HBasePrepared) c.getPrepared();
                    hp.setRegionName(hri.getRegionName());
                    commands.add(new CommandWrapper(c, newSession));
                } else {
                    commands.add(commandProxy.getCommandInterface(originalSession.getOriginalProperties(),
                            hri.getRegionServerURL(), CommandProxy.createSQL(hri.getRegionName(), planSQL())));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private HBaseSession createHBaseSession() {
        //必须使用新Session，否则在并发执行Command类的executeUpdate或executeQuery时因为使用session对象来同步所以会造成死锁
        HBaseSession newSession = (HBaseSession) originalSession.getDatabase().createSession(originalSession.getUser());
        newSession.setRegionServer(originalSession.getRegionServer());
        return newSession;
    }

    private String planSQL() {
        if (originalPrepared.isQuery() && ((Select) originalPrepared).isGroupQuery())
            return ((Select) originalPrepared).getPlanSQL(true);
        else
            return sql;
    }

    @Override
    public String toString() {
        return sql;
    }

    @Override
    public int getCommandType() {
        return originalPrepared.getType();
    }

    @Override
    public boolean isQuery() {
        return originalPrepared.isQuery();
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return originalPrepared.getParameters();
    }

    @Override
    public ResultInterface executeQuery(final int maxRows, final boolean scrollable) {
        int size = commands.size();
        for (int i = 0; i < size; i++) {
            commands.get(i).setFetchSize(fetchSize);
        }
        Select originalSelect = (Select) originalPrepared;
        //originalSelect.isGroupQuery()如果是false，那么按org.apache.hadoop.hbase.client.ClientScanner的功能来实现。
        //只要Select语句中出现聚合函数、groupBy、Having三者之一都被认为是GroupQuery，
        //对于GroupQuery需要把Select语句同时发给相关的RegionServer，得到结果后再合并。
        if (!originalSelect.isGroupQuery())
            return new HBaseSerializedResult(commands, maxRows, scrollable);

        List<Future<ResultInterface>> futures = New.arrayList(size);
        List<ResultInterface> results = new ArrayList<ResultInterface>(size);
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

        String newSQL = originalSelect.getPlanSQL(true);
        Select newSelect = (Select) createHBaseSession().prepare(newSQL, true);

        return new HBaseMergedResult(results, newSelect, originalSelect, originalSelect.isGroupQuery());
    }

    @Override
    public int executeUpdate() {
        int updateCount = 0;
        int size = commands.size();
        List<Future<Integer>> futures = New.arrayList(size);
        for (int i = 0; i < size; i++) {
            final CommandInterface c = commands.get(i);
            c.setFetchSize(fetchSize);
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

    @Override
    public void close() {
        for (CommandInterface c : commands)
            c.close();
    }

    @Override
    public void cancel() {
        for (CommandInterface c : commands)
            c.cancel();
    }

    @Override
    public ResultInterface getMetaData() {
        return originalPrepared.getCommand().getMetaData();
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }
}

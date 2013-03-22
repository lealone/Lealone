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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Threads;

import com.codefollower.lealone.command.Command;
import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.command.dml.Select;
import com.codefollower.lealone.constant.SysProperties;
import com.codefollower.lealone.expression.ParameterInterface;
import com.codefollower.lealone.hbase.command.merge.HBaseMergedResult;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.result.HBaseSerializedResult;
import com.codefollower.lealone.hbase.util.HBaseRegionInfo;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.result.ResultInterface;
import com.codefollower.lealone.util.New;

public class CommandParallel implements CommandInterface {
    private static ThreadPoolExecutor pool;
    private final HBaseSession originalSession;
    private final Prepared originalPrepared;
    private final String sql;
    private final List<CommandInterface> commands; //保证不会为null且size>=2

    private Long startTimestamp;
    private List<byte[]> rowKeys;

    public CommandParallel(HBaseSession originalSession, CommandProxy commandProxy, //
            byte[] tableName, List<byte[]> startKeys, String sql, Prepared originalPrepared) {
        if (startKeys == null)
            throw new RuntimeException("startKeys is null");
        else if (startKeys.size() < 2)
            throw new RuntimeException("startKeys.size() < 2");

        this.originalSession = originalSession;
        this.originalPrepared = originalPrepared;
        this.sql = sql;
        this.commands = new ArrayList<CommandInterface>(startKeys.size());

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
            for (byte[] startKey : startKeys) {
                HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, startKey);
                if (CommandProxy.isLocal(originalSession, hri)) {
                    HBaseSession newSession = createHBaseSession();
                    Command c = newSession.prepareLocal(planSQL());
                    HBasePrepared hp = (HBasePrepared) c.getPrepared();
                    hp.setRegionName(hri.getRegionName());
                    commands.add(new CommandWrapper(c, newSession)); //newSession在Command关闭的时候自动关闭
                } else {
                    commands.add(commandProxy.getCommandInterface(hri.getRegionServerURL(),
                            CommandProxy.createSQL(hri.getRegionName(), planSQL())));
                }
            }

            //设置默认fetchSize，当执行Update、Delete之类的操作时也需要先抓取记录然后再判断记录是否满足条件。
            setFetchSize(SysProperties.SERVER_RESULT_SET_FETCH_SIZE);
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
        Select originalSelect = (Select) originalPrepared;
        //originalSelect.isGroupQuery()如果是false，那么按org.apache.hadoop.hbase.client.ClientScanner的功能来实现。
        //只要Select语句中出现聚合函数、groupBy、Having三者之一都被认为是GroupQuery，
        //对于GroupQuery需要把Select语句同时发给相关的RegionServer，得到结果后再合并。
        if (!originalSelect.isGroupQuery())
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

        String newSQL = originalSelect.getPlanSQL(true);
        Select newSelect = (Select) createHBaseSession().prepare(newSQL, true);

        return new HBaseMergedResult(results, newSelect, originalSelect);
    }

    @Override
    public int executeUpdate() {
        int updateCount = 0;
        int size = commands.size();
        List<Future<Integer>> futures = New.arrayList(size);
        for (int i = 0; i < size; i++) {
            final CommandInterface c = commands.get(i);
            c.setStartTimestamp(startTimestamp);
            futures.add(pool.submit(new Callable<Integer>() {
                public Integer call() throws Exception {
                    return c.executeUpdate();
                }
            }));
        }
        try {
            if (rowKeys == null)
                rowKeys = New.arrayList();
            for (int i = 0; i < size; i++) {
                updateCount += futures.get(i).get();
                rowKeys.addAll(Arrays.asList(commands.get(i).getTransactionalRowKeys()));
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
        return commands.get(0).getFetchSize();
    }

    @Override
    public void setFetchSize(int fetchSize) {
        int size = commands.size();
        for (int i = 0; i < size; i++) {
            commands.get(i).setFetchSize(fetchSize);
        }
    }

    @Override
    public byte[][] getTransactionalRowKeys() {
        return rowKeys.toArray(new byte[0][0]);
    }

    @Override
    public Long getStartTimestamp() {
        return startTimestamp;
    }

    @Override
    public void setStartTimestamp(Long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }
}

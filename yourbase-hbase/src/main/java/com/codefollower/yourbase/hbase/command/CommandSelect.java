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
import java.util.Properties;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

import com.codefollower.yourbase.command.Command;
import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.command.dml.Select;
import com.codefollower.yourbase.expression.ParameterInterface;
import com.codefollower.yourbase.hbase.command.merge.HBaseMergedResult;
import com.codefollower.yourbase.hbase.engine.HBaseSession;
import com.codefollower.yourbase.hbase.util.HBaseRegionInfo;
import com.codefollower.yourbase.hbase.util.HBaseUtils;
import com.codefollower.yourbase.result.ResultInterface;
import com.codefollower.yourbase.util.New;

public class CommandSelect implements CommandInterface {
    Select select;
    byte[] tableName;
    byte[] start;
    byte[] end;
    CommandProxy commandProxy;
    String sql;
    int fetchSize;
    HBaseSession session;

    private List<CommandInterface> commands;
    private List<byte[]> localRegionNames;
    private ThreadPoolExecutor pool;
    //如果是false，那么按org.apache.hadoop.hbase.client.ClientScanner的功能来实现
    //只要Select语句中出现聚合函数、groupBy、Having三者之一都被认为是GroupQuery。
    //对于GroupQuery需要把Select语句同时发给相关的RegionServer，得到结果后再在client一起合并。
    private boolean isGroupQuery = false;

    public CommandSelect() {
    }

    public CommandInterface init() {
        isGroupQuery = select.isGroupQuery();
        List<byte[]> startKeys;
        try {
            //TODO 使用pool并发执行sql
            if (isGroupQuery) {
                this.pool = new ThreadPoolExecutor(1, 20, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                        Threads.newDaemonThreadFactory("HBaseJdbcGroupQueryCommand"));
                ((ThreadPoolExecutor) this.pool).allowCoreThreadTimeOut(true);
                startKeys = HBaseUtils.getStartKeysInRange(tableName, start, end);
            } else {
                startKeys = new ArrayList<byte[]>(1);
                startKeys.add(start);
            }

            if (startKeys != null && startKeys.size() == 1) {
                byte[] startKey = startKeys.get(0);

                HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, startKey);
                if (CommandProxy.isLocal(session, hri)) {
                    session.setRegionName(Bytes.toBytes(hri.getRegionName()));
                    return session.prepareLocal(sql);
                } else {
                    Properties info = new Properties(session.getOriginalProperties());
                    info.setProperty("REGION_NAME", hri.getRegionName());
                    return commandProxy.getCommandInterface(info, hri.getRegionServerURL(), getNewSQL(select, startKey, false));
                }
            }

            if (startKeys != null && startKeys.size() > 0) {
                commands = new ArrayList<CommandInterface>();
                localRegionNames = New.arrayList(startKeys.size());
                for (byte[] startKey : startKeys) {
                    HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, startKey);
                    if (CommandProxy.isLocal(session, hri)) {
                        Command c = session.prepareLocal(getNewSQL(select, startKey, true));
                        localRegionNames.add(Bytes.toBytes(hri.getRegionName()));
                        commands.add(c);
                    } else {
                        localRegionNames.add(null);
                        Properties info = new Properties(session.getOriginalProperties());
                        info.setProperty("REGION_NAME", hri.getRegionName());
                        commands.add(commandProxy.getCommandInterface(info, hri.getRegionServerURL(),
                                getNewSQL(select, startKey, true)));
                    }
                }
            }
            return this;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    String getNewSQL(Select select, byte[] startKey, boolean isDistributed) {
        return select.getPlanSQL(isDistributed);
    }

    @Override
    public String toString() {
        return sql;
    }

    @Override
    public int getCommandType() {
        return UNKNOWN;
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        if (commands != null)
            for (CommandInterface c : commands)
                return c.getParameters();
        return null;
    }

    @Override
    public ResultInterface executeQuery(int maxRows, boolean scrollable) {
        if (commands != null) {
            List<ResultInterface> results = new ArrayList<ResultInterface>(commands.size());
            int index = 0;
            for (CommandInterface c : commands) {
                if (localRegionNames.get(index) != null)
                    session.setRegionName(localRegionNames.get(index));
                index++;
                ResultInterface result = c.executeQuery(maxRows, scrollable);
                if (result != null)
                    results.add(result);
            }
            String newSQL = select.getPlanSQL(true);
            Select newSelect = (Select) session.prepare(newSQL, true);
            return new HBaseMergedResult(results, newSelect, select, isGroupQuery);
        }
        return null;
    }

    @Override
    public int executeUpdate() {
        return 0;
    }

    @Override
    public void close() {
        if (commands != null)
            for (CommandInterface c : commands)
                c.close();
    }

    @Override
    public void cancel() {
        if (commands != null)
            for (CommandInterface c : commands)
                c.cancel();
    }

    @Override
    public ResultInterface getMetaData() {
        if (commands != null)
            for (CommandInterface c : commands)
                return c.getMetaData();
        return null;
    }
}

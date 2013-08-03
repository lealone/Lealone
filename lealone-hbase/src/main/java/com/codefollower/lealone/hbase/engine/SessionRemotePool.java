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
package com.codefollower.lealone.hbase.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.codefollower.lealone.command.CommandRemote;
import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.engine.ConnectionInfo;
import com.codefollower.lealone.engine.SessionRemote;
import com.codefollower.lealone.expression.Parameter;
import com.codefollower.lealone.expression.ParameterInterface;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.transaction.Transaction;

public class SessionRemotePool {
    private static final int corePoolSize = HBaseUtils.getConfiguration().getInt(HBaseConstants.SESSION_CORE_POOL_SIZE,
            HBaseConstants.DEFAULT_SESSION_CORE_POOL_SIZE);

    //key是Master或RegionServer的URL
    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<SessionRemote>> //
    pool = new ConcurrentHashMap<String, ConcurrentLinkedQueue<SessionRemote>>();

    private static ConcurrentLinkedQueue<SessionRemote> getQueue(String url) {
        ConcurrentLinkedQueue<SessionRemote> queue = pool.get(url);
        if (queue == null) {
            //避免多个线程生成不同的ConcurrentLinkedQueue实例
            synchronized (SessionRemotePool.class) {
                queue = pool.get(url);
                if (queue == null) {
                    queue = new ConcurrentLinkedQueue<SessionRemote>();
                    pool.put(url, queue);
                }
            }
        }
        return queue;
    }

    public static SessionRemote getMasterSessionRemote(Properties info) {
        return getSessionRemote(info, HBaseUtils.getMasterURL());
    }

    public static SessionRemote getSessionRemote(Properties info, String url) {
        SessionRemote sr = getQueue(url).poll();

        if (sr == null || sr.isClosed()) {
            byte[] userPasswordHash = null;
            byte[] filePasswordHash = null;
            Properties prop = new Properties();
            String key;
            for (Object o : info.keySet()) {
                key = o.toString();

                if (key.equalsIgnoreCase("_userPasswordHash_"))
                    userPasswordHash = (byte[]) info.get(key);
                else if (key.equalsIgnoreCase("_filePasswordHash_"))
                    filePasswordHash = (byte[]) info.get(key);
                else
                    prop.setProperty(key, info.getProperty(key));

            }
            ConnectionInfo ci = new ConnectionInfo(url, prop);
            ci.setUserPasswordHash(userPasswordHash);
            ci.setFilePasswordHash(filePasswordHash);
            sr = (SessionRemote) new SessionRemote(ci).connectEmbeddedOrServer(false);
        }

        return sr;
    }

    public static void release(SessionRemote sr) {
        if (sr == null || sr.isClosed())
            return;

        ConcurrentLinkedQueue<SessionRemote> queue = getQueue(sr.getURL());
        if (queue.size() > corePoolSize)
            sr.close();
        else
            queue.offer(sr);
    }

    public static CommandRemote getCommandRemote(HBaseSession originalSession, Prepared prepared, //
            String url, String sql) throws Exception {
        SessionRemote sessionRemote = originalSession.getSessionRemote(url);
        if (sessionRemote != null && sessionRemote.isClosed())
            sessionRemote = null;
        boolean isNew = false;
        if (sessionRemote == null) {
            isNew = true;
            sessionRemote = getSessionRemote(originalSession.getOriginalProperties(), url);
        }

        if (sessionRemote.getTransaction() == null) {
            Transaction t = new Transaction();
            t.setAutoCommit(originalSession.getAutoCommit());
            sessionRemote.setTransaction(t);
        }

        if (isNew)
            originalSession.addSessionRemote(url, sessionRemote);

        return getCommandRemote(sessionRemote, sql, prepared.getParameters(), prepared.getFetchSize());
    }

    public static CommandRemote getCommandRemote(SessionRemote sr, String sql, List<Parameter> parameters, int fetchSize) {
        CommandRemote cr = (CommandRemote) sr.prepareCommand(sql, fetchSize);

        //传递最初的参数值到新的CommandRemote
        if (parameters != null) {
            ArrayList<? extends ParameterInterface> newParams = cr.getParameters();
            for (int i = 0, size = parameters.size(); i < size; i++) {
                newParams.get(i).setValue(parameters.get(i).getParamValue(), true);
            }
        }

        return cr;
    }
}

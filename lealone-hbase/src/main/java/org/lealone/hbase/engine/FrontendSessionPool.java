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
package org.lealone.hbase.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.lealone.api.ParameterInterface;
import org.lealone.command.FrontendCommand;
import org.lealone.command.Prepared;
import org.lealone.engine.ConnectionInfo;
import org.lealone.engine.FrontendSession;
import org.lealone.expression.Parameter;
import org.lealone.hbase.util.HBaseUtils;

public class FrontendSessionPool {
    private static final int corePoolSize = HBaseUtils.getConfiguration().getInt(HBaseConstants.SESSION_CORE_POOL_SIZE,
            HBaseConstants.DEFAULT_SESSION_CORE_POOL_SIZE);

    //key是Master或RegionServer的URL
    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<FrontendSession>> //
    pool = new ConcurrentHashMap<String, ConcurrentLinkedQueue<FrontendSession>>();

    private static ConcurrentLinkedQueue<FrontendSession> getQueue(String url) {
        ConcurrentLinkedQueue<FrontendSession> queue = pool.get(url);
        if (queue == null) {
            //避免多个线程生成不同的ConcurrentLinkedQueue实例
            synchronized (FrontendSessionPool.class) {
                queue = pool.get(url);
                if (queue == null) {
                    queue = new ConcurrentLinkedQueue<FrontendSession>();
                    pool.put(url, queue);
                }
            }
        }
        return queue;
    }

    public static FrontendSession getMasterSessionRemote(Properties info) {
        return getSessionRemote(info, HBaseUtils.getMasterURL());
    }

    public static FrontendSession getSessionRemote(Properties info, String url) {
        FrontendSession sr = getQueue(url).poll();

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
            sr = (FrontendSession) new FrontendSession(ci).connectEmbeddedOrServer(false);
        }

        return sr;
    }

    public static void release(FrontendSession sr) {
        if (sr == null || sr.isClosed())
            return;

        ConcurrentLinkedQueue<FrontendSession> queue = getQueue(sr.getURL());
        if (queue.size() > corePoolSize)
            sr.close();
        else
            queue.offer(sr);
    }

    public static FrontendCommand getCommandRemote(HBaseSession originalSession, Prepared prepared, //
            String url, String sql) throws Exception {
        FrontendSession sessionRemote = originalSession.getSessionRemote(url);
        if (sessionRemote != null && sessionRemote.isClosed())
            sessionRemote = null;
        boolean isNew = false;
        if (sessionRemote == null) {
            isNew = true;
            sessionRemote = getSessionRemote(originalSession.getOriginalProperties(), url);
        }

        if (sessionRemote.getTransaction() == null)
            sessionRemote.setTransaction(originalSession.getTransaction());

        if (isNew)
            originalSession.addSessionRemote(url, sessionRemote);

        return getCommandRemote(sessionRemote, sql, prepared.getParameters(), prepared.getFetchSize());
    }

    public static FrontendCommand getCommandRemote(FrontendSession sr, String sql, List<Parameter> parameters, int fetchSize) {
        FrontendCommand cr = (FrontendCommand) sr.prepareCommand(sql, fetchSize);

        //传递最初的参数值到新的CommandRemote
        if (parameters != null) {
            ArrayList<? extends ParameterInterface> newParams = cr.getParameters();
            for (int i = 0, size = parameters.size(); i < size; i++) {
                newParams.get(i).setValue(parameters.get(i).getParamValue(), true);
            }
        }

        return cr;
    }

    public static void check() {
        for (ConcurrentLinkedQueue<FrontendSession> queue : pool.values())
            for (FrontendSession sr : queue)
                sr.checkTransfers();
    }
}

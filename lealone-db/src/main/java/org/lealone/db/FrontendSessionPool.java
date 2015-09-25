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
package org.lealone.db;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.lealone.client.FrontendCommand;
import org.lealone.client.FrontendSession;
import org.lealone.common.message.DbException;
import org.lealone.sql.PreparedInterface;

public class FrontendSessionPool {
    private static final int QUEUE_SIZE = 3;

    // key是集群中每个节点的URL
    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<FrontendSession>> pool = new ConcurrentHashMap<>();

    private static ConcurrentLinkedQueue<FrontendSession> getQueue(String url) {
        ConcurrentLinkedQueue<FrontendSession> queue = pool.get(url);
        if (queue == null) {
            // 避免多个线程生成不同的ConcurrentLinkedQueue实例
            synchronized (FrontendSessionPool.class) {
                queue = pool.get(url);
                if (queue == null) {
                    queue = new ConcurrentLinkedQueue<>();
                    pool.put(url, queue);
                }
            }
        }
        return queue;
    }

    public static FrontendSession getFrontendSession(Session originalSession, String url) {
        return getFrontendSession(originalSession, url, true);
    }

    public static FrontendSession getSeedEndpointFrontendSession(Session originalSession, String url) {
        return getFrontendSession(originalSession, url, false);
    }

    private static FrontendSession getFrontendSession(Session originalSession, String url, boolean isLocal) {
        FrontendSession fs = getQueue(url).poll();

        if (fs == null || fs.isClosed()) {
            ConnectionInfo oldCi = originalSession.getConnectionInfo();
            // 未来新加的代码如果忘记设置这两个字段，出问题时方便查找原因
            if (originalSession.getOriginalProperties() == null || oldCi == null)
                throw DbException.throwInternalError();

            ConnectionInfo ci = new ConnectionInfo(url, originalSession.getOriginalProperties());
            ci.setProperty("IS_LOCAL", isLocal ? "true" : "false");
            ci.setUserName(oldCi.getUserName());
            ci.setUserPasswordHash(oldCi.getUserPasswordHash());
            ci.setFilePasswordHash(oldCi.getFilePasswordHash());
            ci.setFileEncryptionKey(oldCi.getFileEncryptionKey());
            fs = (FrontendSession) new FrontendSession(ci).connectEmbeddedOrServer(false);
        }

        return fs;
    }

    public static void release(FrontendSession fs) {
        if (fs == null || fs.isClosed())
            return;

        ConcurrentLinkedQueue<FrontendSession> queue = getQueue(fs.getURL());
        if (queue.size() > QUEUE_SIZE)
            fs.close();
        else
            queue.offer(fs);
    }

    public static FrontendCommand getFrontendCommand(Session originalSession, PreparedInterface prepared, //
            String url, String sql) throws Exception {
        FrontendSession fs = originalSession.getFrontendSession(url);
        if (fs != null && fs.isClosed())
            fs = null;
        boolean isNew = false;
        if (fs == null) {
            isNew = true;
            fs = getFrontendSession(originalSession, url);
        }

        if (fs.getTransaction() == null)
            fs.setTransaction(originalSession.getTransaction());

        if (isNew)
            originalSession.addFrontendSession(url, fs);

        return getFrontendCommand(fs, sql, prepared.getParameters(), prepared.getFetchSize());
    }

    public static FrontendCommand getFrontendCommand(FrontendSession fs, String sql, //
            List<? extends ParameterInterface> parameters, int fetchSize) {
        FrontendCommand fc = (FrontendCommand) fs.prepareCommand(sql, fetchSize);

        // 传递最初的参数值到新的FrontendCommand
        if (parameters != null) {
            ArrayList<? extends ParameterInterface> newParams = fc.getParameters();
            // SQL重写后可能没有占位符了
            if (!newParams.isEmpty()) {
                if (SysProperties.CHECK && newParams.size() != parameters.size())
                    throw DbException.throwInternalError();
                for (int i = 0, size = parameters.size(); i < size; i++) {
                    newParams.get(i).setValue(parameters.get(i).getParamValue(), true);
                }
            }
        }

        return fc;
    }

    public static void check() {
        for (ConcurrentLinkedQueue<FrontendSession> queue : pool.values())
            for (FrontendSession sr : queue)
                sr.checkTransfers();
    }
}

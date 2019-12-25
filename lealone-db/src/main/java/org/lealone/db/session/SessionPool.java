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
package org.lealone.db.session;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Command;
import org.lealone.db.CommandParameter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.SysProperties;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.Session;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLCommand;

public class SessionPool {
    private static final int QUEUE_SIZE = 3;

    // key是集群中每个节点的URL
    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<Session>> pool = new ConcurrentHashMap<>();

    private static ConcurrentLinkedQueue<Session> getQueue(String url) {
        ConcurrentLinkedQueue<Session> queue = pool.get(url);
        if (queue == null) {
            // 避免多个线程生成不同的ConcurrentLinkedQueue实例
            synchronized (SessionPool.class) {
                queue = pool.get(url);
                if (queue == null) {
                    queue = new ConcurrentLinkedQueue<>();
                    pool.put(url, queue);
                }
            }
        }
        return queue;
    }

    public static Session getSession(ServerSession originalSession, String url) {
        return getSession(originalSession, url, true);
    }

    public static Session getSession(ServerSession originalSession, String url, boolean remote) {
        Session session = remote ? getQueue(url).poll() : null; // 在本地创建session时不用从缓存队列中找

        if (session == null || session.isClosed()) {
            ConnectionInfo oldCi = originalSession.getConnectionInfo();
            // 未来新加的代码如果忘记设置这个字段，出问题时方便查找原因
            if (oldCi == null) {
                throw DbException.throwInternalError();
            }

            ConnectionInfo ci = new ConnectionInfo(url, oldCi.getProperties());
            ci.setProperty("IS_LOCAL", "true");
            ci.setUserName(oldCi.getUserName());
            ci.setUserPasswordHash(oldCi.getUserPasswordHash());
            ci.setFilePasswordHash(oldCi.getFilePasswordHash());
            ci.setFileEncryptionKey(oldCi.getFileEncryptionKey());
            ci.setRemote(remote);
            // 因为已经精确知道要连哪个节点了，connect不用考虑运行模式，所以用false
            session = ci.createSession().connect(false);
        }
        return session;
    }

    public static void getSessionAsync(ServerSession originalSession, String url,
            AsyncHandler<AsyncResult<Session>> asyncHandler) {
        getSessionAsync(originalSession, url, true, asyncHandler);
    }

    public static void getSessionAsync(ServerSession originalSession, String url, boolean remote,
            AsyncHandler<AsyncResult<Session>> asyncHandler) {
        ConnectionInfo oldCi = originalSession.getConnectionInfo();
        // 未来新加的代码如果忘记设置这个字段，出问题时方便查找原因
        if (oldCi == null) {
            throw DbException.throwInternalError();
        }

        ConnectionInfo ci = new ConnectionInfo(url, oldCi.getProperties());
        ci.setProperty("IS_LOCAL", "true");
        ci.setUserName(oldCi.getUserName());
        ci.setUserPasswordHash(oldCi.getUserPasswordHash());
        ci.setFilePasswordHash(oldCi.getFilePasswordHash());
        ci.setFileEncryptionKey(oldCi.getFileEncryptionKey());
        ci.setRemote(remote);
        // 因为已经精确知道要连哪个节点了，connect不用考虑运行模式，所以用false
        ci.createSession().connectAsync(false, asyncHandler);
    }

    public static void release(Session session) {
        if (session == null || session.isClosed())
            return;

        ConcurrentLinkedQueue<Session> queue = getQueue(session.getURL());
        if (queue.size() > QUEUE_SIZE)
            session.close();
        else
            queue.offer(session);
    }

    @Deprecated
    public static Command getCommand(ServerSession originalSession, PreparedSQLStatement prepared, //
            String url, String sql) throws Exception {
        Session session = originalSession.getSession(url);
        if (session != null && session.isClosed())
            session = null;
        boolean isNew = false;
        if (session == null) {
            isNew = true;
            session = getSession(originalSession, url, true);
        }

        if (session.getParentTransaction() == null)
            session.setParentTransaction(originalSession.getTransaction());

        if (isNew)
            originalSession.addSession(url, session);

        List<? extends CommandParameter> parameters = prepared.getParameters();
        int fetchSize = prepared.getFetchSize();
        if (parameters == null || parameters.isEmpty())
            return session.createSQLCommand(sql, fetchSize);

        SQLCommand command = session.prepareSQLCommand(sql, fetchSize);

        // 传递最初的参数值到新的Command
        if (parameters != null) {
            List<? extends CommandParameter> newParams = command.getParameters();
            // SQL重写后可能没有占位符了
            if (!newParams.isEmpty()) {
                if (SysProperties.CHECK && newParams.size() != parameters.size())
                    throw DbException.throwInternalError();
                for (int i = 0, size = parameters.size(); i < size; i++) {
                    newParams.get(i).setValue(parameters.get(i).getValue(), true);
                }
            }
        }

        return command;
    }
}

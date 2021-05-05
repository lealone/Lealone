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
package org.lealone.db.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.lealone.db.DbObjectType;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionStatus;
import org.lealone.transaction.Transaction;

//数据库对象模型已经支持多版本，所以对象锁只需要像行锁一样实现即可
public class DbObjectLockImpl implements DbObjectLock {

    private final AtomicReference<ServerSession> ref = new AtomicReference<>();
    private final DbObjectType type;
    private ArrayList<AsyncHandler<AsyncResult<Boolean>>> handlers;
    private ConcurrentLinkedQueue<ServerSession> waitingSessions = new ConcurrentLinkedQueue<>();
    private List<String> retryReplicationNames;

    public DbObjectLockImpl(DbObjectType type) {
        this.type = type;
    }

    @Override
    public DbObjectType getDbObjectType() {
        return type;
    }

    @Override
    public void addHandler(AsyncHandler<AsyncResult<Boolean>> handler) {
        if (handlers == null)
            handlers = new ArrayList<>(1);
        handlers.add(handler);
    }

    @Override
    public boolean lock(ServerSession session, boolean exclusive) {
        if (exclusive)
            return tryExclusiveLock(session);
        else
            return trySharedLock(session);
    }

    private void addWaitingTransaction(ServerSession lockOwner, ServerSession session) {
        if (lockOwner != null) {
            Transaction transaction = lockOwner.getTransaction();
            if (transaction != null) {
                transaction.addWaitingTransaction(this, session.getTransaction(), Transaction.getTransactionListener());
            }
        }
    }

    @Override
    public boolean trySharedLock(ServerSession session) {
        return true;
    }

    @Override
    public boolean tryExclusiveLock(ServerSession session) {
        if (retryReplicationNames == null || retryReplicationNames.isEmpty()) {
            if (ref.compareAndSet(null, session)) {
                session.addLock(this);
                return true;
            } else {
                ServerSession oldSession = ref.get();
                if (oldSession != session) {
                    addWaitingTransaction(oldSession, session);
                    waitingSessions.add(session);
                    return false;
                } else {
                    return true;
                }
            }
        } else {
            String name = retryReplicationNames.get(0);
            if (name.equals(session.getReplicationName())) {
                session.addLock(this);
                retryReplicationNames.remove(0);
                return true;
            } else {
                waitingSessions.add(session);
                return false;
            }
        }
    }

    @Override
    public void unlock(ServerSession session) {
        unlock(session, true, null);
    }

    @Override
    public void unlock(ServerSession session, boolean succeeded) {
        unlock(session, succeeded, null);
    }

    @Override
    public void unlock(ServerSession oldSession, boolean succeeded, ServerSession newSession) {
        if (ref.compareAndSet(oldSession, newSession)) {
            if (handlers != null) {
                handlers.forEach(h -> {
                    h.handle(new AsyncResult<>(succeeded));
                });
                handlers = null;
            }

            if (newSession != null) {
                newSession.addLock(this);
                waitingSessions.remove(newSession);
                tryExclusiveLock(oldSession);
                newSession.setStatus(SessionStatus.RETRYING);
            } else {
                if (retryReplicationNames == null || retryReplicationNames.isEmpty()) {
                    for (ServerSession s : waitingSessions)
                        s.setStatus(SessionStatus.RETRYING_RETURN_RESULT);
                    waitingSessions = new ConcurrentLinkedQueue<>();
                } else {
                    String name = retryReplicationNames.get(0);
                    for (ServerSession s : waitingSessions) {
                        if (name.equals(s.getReplicationName())) {
                            s.setStatus(SessionStatus.RETRYING_RETURN_RESULT);
                            waitingSessions.remove(s);
                            retryReplicationNames.remove(0);
                            break;
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean isLockedExclusively() {
        return ref.get() != null;
    }

    @Override
    public boolean isLockedExclusivelyBy(ServerSession session) {
        return ref.get() == session;
    }

    @Override
    public void setRetryReplicationNames(List<String> retryReplicationNames) {
        this.retryReplicationNames = retryReplicationNames;
    }
}

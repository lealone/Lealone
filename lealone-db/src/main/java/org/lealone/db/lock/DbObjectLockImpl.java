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
import java.util.concurrent.atomic.AtomicReference;

import org.lealone.db.DbObjectType;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.ServerSession;
import org.lealone.storage.replication.ReplicationConflictType;
import org.lealone.transaction.Transaction;

//数据库对象模型已经支持多版本，所以对象锁只需要像行锁一样实现即可
public class DbObjectLockImpl implements DbObjectLock {

    private final AtomicReference<ServerSession> ref = new AtomicReference<>();
    private final DbObjectType type;
    private ArrayList<AsyncHandler<AsyncResult<Boolean>>> handlers;

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

                // 在复制模式下执行时用得着
                if (session.getReplicationName() != null)
                    session.setLockedExclusivelyBy(lockOwner, ReplicationConflictType.DB_OBJECT_LOCK);
            }
        }
    }

    @Override
    public boolean trySharedLock(ServerSession session) {
        return true;
    }

    @Override
    public boolean tryExclusiveLock(ServerSession session) {
        if (ref.compareAndSet(null, session)) {
            session.addLock(this);
            return true;
        } else {
            ServerSession oldSession = ref.get();
            if (oldSession != session) {
                addWaitingTransaction(oldSession, session);
                return false;
            } else {
                return true;
            }
        }
    }

    @Override
    public void unlock(ServerSession session) {
        unlock(session, true);
    }

    @Override
    public void unlock(ServerSession session, boolean succeeded) {
        if (ref.compareAndSet(session, null)) {
            if (handlers != null) {
                handlers.forEach(h -> {
                    h.handle(new AsyncResult<>(succeeded));
                });
                handlers = null;
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
}

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
package org.lealone.db.table;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.index.Index;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.transaction.Transaction;

public class LockTable extends Table {

    private final Lock sharedLock;
    private final Lock exclusiveLock;
    private final ConcurrentHashMap<ServerSession, AtomicInteger> sharedSessions = new ConcurrentHashMap<>();
    private volatile ServerSession exclusiveSession;
    private ArrayList<AsyncHandler<AsyncResult<Boolean>>> handlers;

    public LockTable(Schema schema, String name) {
        super(schema, -1, name, false, false);
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        sharedLock = lock.readLock();
        exclusiveLock = lock.writeLock();
    }

    public void addHandler(AsyncHandler<AsyncResult<Boolean>> handler) {
        if (handlers == null)
            handlers = new ArrayList<>(1);
        handlers.add(handler);
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public TableType getTableType() {
        return TableType.LOCK_TABLE;
    }

    @Override
    public Index getScanIndex(ServerSession session) {
        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
    }

    @Override
    public long getMaxDataModificationId() {
        return 0;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public boolean canDrop() {
        return false;
    }

    @Override
    public boolean canGetRowCount() {
        return false;
    }

    @Override
    public long getRowCount(ServerSession session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        return 0;
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
        if (sharedLock.tryLock()) {
            AtomicInteger counter = sharedSessions.get(session);
            if (counter == null) {
                counter = new AtomicInteger();
                sharedSessions.put(session, counter);
                // 如果先获得排它锁，就不用加到session的锁列表中了
                if (exclusiveSession != session) {
                    session.addLock(this);
                }
            }
            counter.incrementAndGet();
            return true;
        } else {
            addWaitingTransaction(exclusiveSession, session);
            return false;
        }
    }

    @Override
    public boolean tryExclusiveLock(ServerSession session) {
        if (exclusiveLock.tryLock()) {
            // 不用处理session是否在sharedSessions中的情况，因为如果先要到共享锁，排它锁是要不到的
            if (exclusiveSession != session) {
                exclusiveSession = session;
                session.addLock(this);
            } else {
                exclusiveLock.unlock(); // 要了两次排他锁，直接释放
            }
            return true;
        } else {
            // 先要到共享锁，再要排他锁时会失败，但是不能自己等自己
            if (!sharedSessions.containsKey(session)) {
                for (ServerSession sharedSession : sharedSessions.keySet()) {
                    addWaitingTransaction(sharedSession, session);
                }
            }
            return false;
        }
    }

    @Override
    public void unlock(ServerSession session) {
        unlock(session, true);
    }

    @Override
    public void unlock(ServerSession session, boolean succeeded) {
        if (handlers != null) {
            handlers.forEach(h -> {
                h.handle(new AsyncResult<>(succeeded));
            });
            handlers = null;
        }
        AtomicInteger counter = sharedSessions.remove(session);
        if (counter != null) {
            for (int i = 0; i < counter.get(); i++) {
                sharedLock.unlock();
            }
        }
        if (exclusiveSession == session) {
            exclusiveSession = null;
            exclusiveLock.unlock();
        }
    }

    @Override
    public boolean isLockedExclusively() {
        return exclusiveSession != null;
    }

    @Override
    public boolean isLockedExclusivelyBy(ServerSession session) {
        return exclusiveSession == session;
    }
}

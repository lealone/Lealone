/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.util.HashMap;

import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.session.ServerSession;
import org.lealone.transaction.Transaction;

public class TransactionalDbObjects<T extends DbObject> {

    private final HashMap<String, T> dbObjects;

    private final TransactionalDbObjects<T> old;
    private long version;
    private long parentVersion;

    public TransactionalDbObjects(HashMap<String, T> dbObjects) {
        this.dbObjects = dbObjects;
        this.old = null;
    }

    public TransactionalDbObjects(ServerSession session, HashMap<String, T> dbObjects, TransactionalDbObjects<T> old) {
        this.dbObjects = dbObjects;
        this.old = old;
        version = session.getTransaction().getTransactionId();

        // 在sharding模式下，如果接入节点也是数据库的目标节点之一，那么允许接入节点的事务访问子事务未提交的对象，
        // 因为sql在准备阶段是在接入节点的事务中执行的，会访问到模式中的表，但是update/query是在子事务中执行的，
        // 由子事务把对象加入dbObjects，用的是子事务的事务ID当version.
        if (session.getParentTransaction() != null)
            parentVersion = session.getParentTransaction().getTransactionId();
    }

    public HashMap<String, T> getDbObjects() {
        return dbObjects;
    }

    public T find(ServerSession session, String dbObjectName) {
        if (session == null) {
            if (version <= 0)
                return dbObjects.get(dbObjectName);
            else if (old != null) {
                return old.find(session, dbObjectName);
            } else {
                return dbObjects.get(dbObjectName);
            }
        }

        Transaction transaction = session.getTransaction();
        long tid = transaction.getTransactionId();
        if (tid == version || tid == parentVersion) {
            return dbObjects.get(dbObjectName);
        }
        switch (transaction.getIsolationLevel()) {
        case Transaction.IL_REPEATABLE_READ:
        case Transaction.IL_SERIALIZABLE:
            if (tid >= version)
                return dbObjects.get(dbObjectName);
            else if (old != null) {
                return old.find(session, dbObjectName);
            }
            return dbObjects.get(dbObjectName);
        case Transaction.IL_READ_COMMITTED:
            if (version <= 0)
                return dbObjects.get(dbObjectName);
            else if (old != null) {
                return old.find(session, dbObjectName);
            }
        default:
            return dbObjects.get(dbObjectName);
        }
    }

    @SuppressWarnings("unchecked")
    public void add(DbObject dbObject) {
        dbObjects.put(dbObject.getName(), (T) dbObject);
    }

    public boolean containsKey(ServerSession session, String dbObjectName) {
        return find(session, dbObjectName) != null;
    }

    public void remove(String dbObjectName) {
        dbObjects.remove(dbObjectName);
    }

    public TransactionalDbObjects<T> copy(ServerSession session) {
        HashMap<String, T> dbObjects = new CaseInsensitiveMap<>(this.dbObjects);
        return new TransactionalDbObjects<>(session, dbObjects, this);
    }

    public TransactionalDbObjects<T> commit() {
        version = 0;
        parentVersion = 0;
        return this;
    }

    public TransactionalDbObjects<T> rollback() {
        return old;
    }
}

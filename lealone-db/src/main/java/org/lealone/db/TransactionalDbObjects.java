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

//只存放同一种DbObject的多个实例，支持多版本
//需要配合DbObjectLock使用，只允许一个事务修改
public class TransactionalDbObjects {

    private HashMap<String, DbObject> dbObjects;
    private TransactionalDbObjects old;
    private long version;
    private long parentVersion;

    public TransactionalDbObjects() {
        this.dbObjects = new CaseInsensitiveMap<>();
    }

    public HashMap<String, DbObject> getDbObjects() {
        return dbObjects;
    }

    public boolean containsKey(ServerSession session, String dbObjectName) {
        return find(session, dbObjectName) != null;
    }

    public DbObject find(ServerSession session, String dbObjectName) {
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

    public void add(DbObject dbObject) {
        dbObjects.put(dbObject.getName(), dbObject);
    }

    public void copyOnAdd(ServerSession session, DbObject dbObject) {
        copy(session);
        add(dbObject);
    }

    public void remove(String dbObjectName) {
        dbObjects.remove(dbObjectName);
    }

    public void copyOnRemove(ServerSession session, String dbObjectName) {
        copy(session);
        remove(dbObjectName);
    }

    private void copy(ServerSession session) {
        long tid = session.getTransaction().getTransactionId();
        // 如果是当前事务就不需要重复copy了
        if (version != tid) {
            TransactionalDbObjects old = new TransactionalDbObjects();
            old.dbObjects.putAll(dbObjects);
            old.old = this.old;
            this.old = old;
            version = tid;
            // 在sharding模式下，如果接入节点也是数据库的目标节点之一，那么允许接入节点的事务访问子事务未提交的对象，
            // 因为sql在准备阶段是在接入节点的事务中执行的，会访问到模式中的表，但是update/query是在子事务中执行的，
            // 由子事务把对象加入dbObjects，用的是子事务的事务ID当version.
            if (session.getParentTransaction() != null)
                parentVersion = session.getParentTransaction().getTransactionId();
        }
    }

    public void commit() {
        if (old != null) {
            old.version = version;
            old.parentVersion = parentVersion;
        }
        version = 0;
        parentVersion = 0;
    }

    public void rollback() {
        if (old != null) {
            dbObjects = old.dbObjects;
            old = old.old;
        }
        version = 0;
        parentVersion = 0;
    }
}

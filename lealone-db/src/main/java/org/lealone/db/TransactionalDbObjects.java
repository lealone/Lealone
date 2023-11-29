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
import org.lealone.transaction.TransactionEngine;

//只存放同一种DbObject的多个实例，支持多版本
//需要配合DbObjectLock使用，只允许一个事务修改
public class TransactionalDbObjects {

    private HashMap<String, DbObject> dbObjects;
    private TransactionalDbObjects old;
    private long version;

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
        if (tid == version) {
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
        }
    }

    public void commit() {
        if (old != null) {
            old.version = version;
        }
        version = 0;
    }

    public void rollback() {
        if (old != null) {
            dbObjects = old.dbObjects;
            old = old.old;
        }
        version = 0;
    }

    public void gc(TransactionEngine te) {
        if (old == null)
            return;
        if (!te.containsRepeatableReadTransactions()) {
            old = null;
            return;
        }
        long minTid = Long.MAX_VALUE;
        for (Transaction t : te.currentTransactions()) {
            if (t.isRepeatableRead() && t.getTransactionId() < minTid)
                minTid = t.getTransactionId();
        }
        if (minTid != Long.MAX_VALUE) {
            TransactionalDbObjects last = this;
            TransactionalDbObjects old = this.old;
            while (old != null) {
                if (old.version < minTid) {
                    last.old = null;
                    break;
                }
                last = old;
                old = old.old;
            }
        } else {
            old = null;
        }
    }
}

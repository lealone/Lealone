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

import java.util.HashMap;

import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.session.ServerSession;
import org.lealone.transaction.Transaction;

public class TransactionalDbObjects<T extends DbObject> {

    private final HashMap<String, T> dbObjects;

    private final TransactionalDbObjects<T> old;
    private long version;

    public TransactionalDbObjects(HashMap<String, T> dbObjects) {
        this.dbObjects = dbObjects;
        this.old = null;
    }

    public TransactionalDbObjects(ServerSession session, HashMap<String, T> dbObjects, TransactionalDbObjects<T> old) {
        this.dbObjects = dbObjects;
        this.old = old;
        version = session.getTransaction().getTransactionId();
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
        if (transaction.getTransactionId() == version) {
            return dbObjects.get(dbObjectName);
        }
        switch (transaction.getIsolationLevel()) {
        case Transaction.IL_REPEATABLE_READ:
        case Transaction.IL_SERIALIZABLE:
            if (transaction.getTransactionId() >= version)
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
        return this;
    }

    public TransactionalDbObjects<T> rollback() {
        return old;
    }
}

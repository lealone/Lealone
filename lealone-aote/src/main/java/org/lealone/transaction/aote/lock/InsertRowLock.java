/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.lock;

import org.lealone.db.session.Session;
import org.lealone.transaction.aote.TransactionalValue;

public class InsertRowLock extends RowLock {

    private final TransactionalValue tv;

    public InsertRowLock(TransactionalValue tv) {
        this.tv = tv;
    }

    @Override
    public boolean isInsert() {
        return true;
    }

    @Override
    public void unlock(Session oldSession, Session newSession) {
        super.unlock(oldSession, newSession);
        tv.resetRowLock();
    }
}

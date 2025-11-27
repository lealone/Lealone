/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.lock;

import com.lealone.transaction.Transaction;

// 只有一个线程修改
public class LockOwner {

    public Transaction getTransaction() {
        return null;
    }

    public Object getOldValue() {
        return null;
    }
}

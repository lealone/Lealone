/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.lock;

public interface Lockable {

    public default void setKey(Object key) {
    }

    // 如果实现类包装了一个被锁的对象，那它就返回这个对象，否则返回自身
    public default Object getValue() {
        return this;
    }

    public void setLockedValue(Object value);

    public Object getLockedValue();

    public Object copy(Object oldLockedValue, Lock lock);

    public Lock getLock();

    public void setLock(Lock lock);

    public boolean compareAndSetLock(Lock expect, Lock update);

}

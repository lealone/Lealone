/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.page;

import com.lealone.db.lock.Lock;
import com.lealone.db.lock.Lockable;

public interface IPageReference {

    Lockable markDirtyPage(Object key, PageListener oldPageListener);

    PageListener getPageListener();

    Lock getLock();

    void remove(Object key);

}

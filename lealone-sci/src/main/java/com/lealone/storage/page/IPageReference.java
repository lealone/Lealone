/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.page;

import com.lealone.db.lock.Lock;

public interface IPageReference {

    boolean markDirtyPage(PageListener oldPageListener);

    PageListener getPageListener();

    Lock getLock();

    void remove(Object key);

}

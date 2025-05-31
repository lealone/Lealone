/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import com.lealone.db.lock.Lock;
import com.lealone.storage.page.PageListener;

public class PageLock extends Lock {

    private volatile PageListener pageListener;

    @Override
    public PageListener getPageListener() {
        return pageListener;
    }

    @Override
    public void setPageListener(PageListener pageListener) {
        this.pageListener = pageListener;
    }

    @Override
    public String getLockType() {
        return getClass().getSimpleName();
    }

    @Override
    public boolean isPageLock() {
        return true;
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.page;

import com.lealone.db.lock.Lock;

public class PageListener {

    private final IPageReference pageReference;
    private PageListener parent;

    public PageListener(IPageReference pageReference) {
        this.pageReference = pageReference;
    }

    public IPageReference getPageReference() {
        return pageReference;
    }

    public boolean isPageStale() {
        return pageReference.getPageListener() != this;
    }

    public PageListener getParent() {
        return parent;
    }

    public void setParent(PageListener parent) {
        this.parent = parent;
    }

    public Lock getLock() {
        return pageReference.getLock();
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.page;

public class PageListener {

    private final IPageReference pageReference;

    public PageListener(IPageReference pageReference) {
        this.pageReference = pageReference;
    }

    public IPageReference getPageReference() {
        return pageReference;
    }

    public boolean isPageStale() {
        return pageReference.getPageListener() != this;
    }
}

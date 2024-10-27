/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.page;

public class PageKey {

    public final Object key;
    public final boolean first;
    public final long pos;

    public PageKey(Object key, boolean first) {
        this.key = key;
        this.first = first;
        this.pos = -1;
    }

    public PageKey(Object key, boolean first, long pos) {
        this.key = key;
        this.first = first;
        this.pos = pos;
    }

    @Override
    public String toString() {
        return "PageKey [key=" + key + ", first=" + first + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (first ? 1231 : 1237);
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PageKey other = (PageKey) obj;
        if (first != other.first)
            return false;
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        return true;
    }
}

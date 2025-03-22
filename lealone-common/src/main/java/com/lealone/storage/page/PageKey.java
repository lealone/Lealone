/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.page;

import java.util.Objects;

public class PageKey {

    public final Object key;
    public final boolean first;
    public final int level;
    public final long pos;

    public PageKey(Object key, boolean first) {
        this(key, first, -1, -1);
    }

    public PageKey(Object key, boolean first, int level) {
        this(key, first, level, -1);
    }

    public PageKey(Object key, boolean first, int level, long pos) {
        this.key = key;
        this.first = first;
        this.level = level;
        this.pos = pos;
    }

    @Override
    public String toString() {
        return "PageKey [key=" + key + ", first=" + first + ", level=" + level + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, key, level);
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
        return first == other.first && Objects.equals(key, other.key) && level == other.level;
    }
}

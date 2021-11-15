/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.index.standard;

import org.lealone.db.value.ValueArray;

public class VersionedValue {

    public final int version; // 表的元数据版本号
    public final ValueArray value;

    public VersionedValue(int version, ValueArray value) {
        this.version = version;
        this.value = value;
    }

    @Override
    public String toString() {
        // StringBuilder buff = new StringBuilder("VersionedValue[ ");
        // buff.append("version = ").append(version);
        // buff.append(", value = ").append(value).append(" ]");
        return value.toString();
    }
}
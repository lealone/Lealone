/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.index.standard;

import java.util.Arrays;

import org.lealone.db.value.Value;

public class VersionedValue {

    public final int version; // 表的元数据版本号
    public final Value[] columns;

    public VersionedValue(int version, Value[] columns) {
        this.version = version;
        this.columns = columns;
    }

    @Override
    public String toString() {
        // StringBuilder buff = new StringBuilder("VersionedValue[ ");
        // buff.append("version = ").append(version);
        // buff.append(", columns = ").append(Arrays.toString(columns)).append(" ]");
        return Arrays.toString(columns);
    }
}
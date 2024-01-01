/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import java.util.Arrays;

import com.lealone.db.value.Value;

public class IndexKey {

    public final Value[] columns;

    public IndexKey(Value[] columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return Arrays.toString(columns);
    }
}
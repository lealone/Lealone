/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import com.lealone.db.DataHandler;
import com.lealone.db.value.CompareMode;

/**
 * 用于优化唯一性检查，包括唯一约束、多字段primary key以及非byte/short/int/long类型的单字段primary key
 * 通过SecondaryIndex增加索引记录时不需要在执行addIfAbsent前后做唯一性检查
 */
public class UniqueKeyType extends IndexKeyType {

    public UniqueKeyType(DataHandler handler, CompareMode compareMode, int[] sortTypes) {
        super(handler, compareMode, sortTypes);
    }

    @Override
    protected boolean isUniqueKey() {
        return true;
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import com.lealone.common.util.MathUtils;
import com.lealone.db.value.ValueLong;

//专门用于StandardPrimaryIndex，它的key是ValueLong且不为null
public class PrimaryKeyType extends ValueDataType {

    public PrimaryKeyType() {
        super(null, null, null);
    }

    @Override
    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        return MathUtils.compareLong(((ValueLong) a).getLong(), ((ValueLong) b).getLong());
    }

    @Override
    public int getMemory(Object obj) {
        return ValueLong.type.getMemory(obj);
    }
}

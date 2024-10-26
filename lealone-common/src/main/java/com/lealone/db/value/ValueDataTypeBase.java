/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.value;

import java.nio.ByteBuffer;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;

public abstract class ValueDataTypeBase implements ValueDataType {

    public abstract int getType();

    public void writeValue(DataBuffer buff, Value v) {
        throw newInternalError();
    }

    @Override
    public Object read(ByteBuffer buff) {
        int tag = buff.get();
        return readValue(buff, tag).getObject();
    }

    public Object read(ByteBuffer buff, int tag) {
        return readValue(buff, tag).getObject();
    }

    public Value readValue(ByteBuffer buff) {
        throw newInternalError();
    }

    public Value readValue(ByteBuffer buff, int tag) {
        return readValue(buff);
    }

    protected IllegalStateException newInternalError() {
        return DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Internal error");
    }
}

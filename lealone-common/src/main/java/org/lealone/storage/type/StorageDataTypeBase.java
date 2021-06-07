/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.type;

import java.nio.ByteBuffer;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.Value;

public abstract class StorageDataTypeBase implements StorageDataType {

    public abstract int getType();

    public abstract void writeValue(DataBuffer buff, Value v);

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

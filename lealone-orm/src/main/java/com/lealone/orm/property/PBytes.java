/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueBytes;
import com.lealone.orm.Model;
import com.lealone.orm.json.Json;

/**
 * byte[] property. 
 */
public class PBytes<M extends Model<M>> extends PBase<M, byte[]> {

    public PBytes(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(byte[] value) {
        return ValueBytes.getNoCopy(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBytes();
    }

    @Override
    protected Object encode() {
        return Json.BASE64_ENCODER.encodeToString(value);
    }

    @Override
    protected byte[] decode(Object v) {
        return Json.BASE64_DECODER.decode(v.toString());
    }
}

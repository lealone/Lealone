/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueJavaObject;
import com.lealone.orm.Model;
import com.lealone.orm.json.Json;

public class PObject<M extends Model<M>> extends PBase<M, Object> {

    public PObject(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(Object value) {
        // 如果是一个字节数组，直接创建即可，不必再序列化
        if (value instanceof byte[])
            return ValueJavaObject.getNoCopy(null, (byte[]) value);
        else
            return ValueJavaObject.getNoCopy(value, null);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getObject();
    }

    @Override
    protected Object encode() {
        if (value instanceof byte[] bytes)
            return Json.BASE64_ENCODER.encodeToString(bytes);
        else
            return Json.encode(value);
    }

    @Override
    protected Object decode(Object v) {
        return Json.decode(v.toString());
    }
}

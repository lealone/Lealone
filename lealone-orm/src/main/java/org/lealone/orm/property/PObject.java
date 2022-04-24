/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueJavaObject;
import org.lealone.orm.Model;
import org.lealone.orm.json.Json;

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
    protected Object encodeValue() {
        return Json.encode(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getObject();
    }

    @Override
    protected void deserialize(Object v) {
        value = Json.decode(v.toString());
    }
}

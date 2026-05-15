/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import java.util.Map;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueNull;
import com.lealone.orm.Model;
import com.lealone.orm.ModelProperty;
import com.lealone.orm.json.JsonFormat;

public abstract class PBase<M extends Model<M>, T> extends ModelProperty<M> {

    protected T value;

    public PBase(String name, M model) {
        super(name, model);
    }

    protected abstract Value createValue(T value);

    private PBase<M, T> p(M model) {
        return this.<PBase<M, T>> getModelProperty(model);
    }

    public final M set(T value) {
        M m = getModel();
        if (m != model) {
            return p(m).set(value);
        }
        // 批量设置NULL
        if (m.isDao() && value == null) {
            expr().set(name, ValueNull.INSTANCE);
        } else if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, value == null ? ValueNull.INSTANCE : createValue(value));
        }
        return model;
    }

    public T get() {
        M m = getModel();
        if (m != model) {
            return p(m).get();
        }
        return value;
    }

    @Override
    protected final void encode(Map<String, Object> map, JsonFormat format) {
        if (value != null) {
            map.put(format.convertName(name), encode(format));
        }
    }

    protected Object encode(JsonFormat format) {
        return encode();
    }

    protected Object encode() {
        return value;
    }

    protected T decode(Object v, JsonFormat format) {
        return decode(v);
    }

    protected T decode(Object v) {
        return null;
    }

    @Override
    protected void decodeAndSet(Object v, JsonFormat format) {
        value = decode(v, format);
        expr().set(name, createValue(value));
    }
}

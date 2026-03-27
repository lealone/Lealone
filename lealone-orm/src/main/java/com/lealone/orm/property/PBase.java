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
import com.lealone.orm.format.JsonFormat;
import com.lealone.orm.format.PropertyFormat;
import com.lealone.orm.format.TypeFormat;

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

    protected abstract TypeFormat<T> getValueFormat(JsonFormat format);

    @Override
    protected final void encode(Map<String, Object> map, JsonFormat format) {
        PropertyFormat pFormat = format.getPropertyFormat();
        if (pFormat != null && (pFormat.encode(map, name, value)))
            return;

        if (value != null) {
            map.put(format.getNameCaseFormat().convert(name), getValueFormat(format).encode(value));
        }
    }

    @Override
    protected void decodeAndSet(Object v, JsonFormat format) {
        value = getValueFormat(format).decode(v);
        expr().set(name, createValue(value));
    }
}

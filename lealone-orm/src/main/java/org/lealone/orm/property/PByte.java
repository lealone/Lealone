/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueByte;
import org.lealone.orm.Model;

/**
 * Byte property.
 *
 * @param <R> the root model bean type
 */
public class PByte<R> extends PBaseNumber<R, Byte> {

    private byte value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PByte(String name, R root) {
        super(name, root);
    }

    private PByte<R> P(Model<?> model) {
        return this.<PByte<R>> getModelProperty(model);
    }

    public final R set(byte value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueByte.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Byte.valueOf(value.toString()).byteValue());
    }

    public final byte get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
        }
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getByte();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        map.put(getName(), value);
    }

    @Override
    protected void deserialize(Object v) {
        value = ((Number) v).byteValue();
    }
}

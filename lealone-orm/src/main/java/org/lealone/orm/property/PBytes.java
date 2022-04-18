/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueJavaObject;
import org.lealone.orm.Model;
import org.lealone.orm.ModelProperty;
import org.lealone.orm.json.Json;

/**
 * byte[] property.
 *
 * @param <R> the root model bean type
 */
public class PBytes<R> extends ModelProperty<R> {

    private byte[] value;

    public PBytes(String name, R root) {
        super(name, root);
    }

    private PBytes<R> P(Model<?> model) {
        return this.<PBytes<R>> getModelProperty(model);
    }

    public R set(byte[] value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueJavaObject.getNoCopy(value, null));
        }
        return root;
    }

    public final byte[] get() {
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBytes();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        if (value != null)
            map.put(getName(), Json.BASE64_ENCODER.encode(value));
    }

    @Override
    protected void deserialize(Object v) {
        value = Json.BASE64_DECODER.decode(v.toString());
    }
}

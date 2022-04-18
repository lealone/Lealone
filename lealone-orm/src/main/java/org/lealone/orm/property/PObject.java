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

public class PObject<M extends Model<M>> extends ModelProperty<M> {

    private Object value;

    public PObject(String name, M model) {
        super(name, model);
    }

    private PObject<M> P(M model) {
        return this.<PObject<M>> getModelProperty(model);
    }

    @Override
    public M set(Object value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueJavaObject.getNoCopy(value, null));
        }
        return model;
    }

    public final Object get() {
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getObject();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        if (value != null) {
            map.put(getName(), Json.encode(value));
        }
    }

    @Override
    protected void deserialize(Object v) {
        value = Json.decode(v.toString());
    }
}

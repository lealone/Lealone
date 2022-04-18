/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Map;
import java.util.UUID;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueUuid;
import org.lealone.orm.Model;

/**
 * UUID property.
 */
public class PUuid<M extends Model<M>> extends PBaseValueEqual<M, UUID> {

    private UUID value;

    public PUuid(String name, M model) {
        super(name, model);
    }

    private PUuid<M> P(M model) {
        return this.<PUuid<M>> getModelProperty(model);
    }

    public M set(UUID value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueUuid.get(value.getMostSignificantBits(), value.getLeastSignificantBits()));
        }
        return model;
    }

    public final UUID get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
        }
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = (UUID) ValueUuid.get(v.getBytesNoCopy()).getObject();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        if (value != null)
            map.put(getName(), value.toString());
    }

    @Override
    protected void deserialize(Object v) {
        value = UUID.fromString(v.toString());
    }
}

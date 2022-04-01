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
 *
 * @param <R> the root model bean type
 */
public class PUuid<R> extends PBaseValueEqual<R, UUID> {

    private UUID value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PUuid(String name, R root) {
        super(name, root);
    }

    private PUuid<R> P(Model<?> model) {
        return this.<PUuid<R>> getModelProperty(model);
    }

    public R set(UUID value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueUuid.get(value.getMostSignificantBits(), value.getLeastSignificantBits()));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(UUID.fromString(value.toString()));
    }

    public final UUID get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
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

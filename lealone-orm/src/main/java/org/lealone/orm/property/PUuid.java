/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.UUID;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueUuid;
import org.lealone.orm.Model;

/**
 * UUID property.
 */
public class PUuid<M extends Model<M>> extends PBaseValueEqual<M, UUID> {

    public PUuid(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(UUID value) {
        return ValueUuid.get(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    @Override
    protected Object encodeValue() {
        return value.toString();
    }

    @Override
    protected void deserialize(Value v) {
        value = (UUID) ValueUuid.get(v.getBytesNoCopy()).getObject();
    }

    @Override
    protected void deserialize(Object v) {
        value = UUID.fromString(v.toString());
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import java.util.UUID;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueUuid;
import com.lealone.orm.Model;
import com.lealone.orm.format.JsonFormat;
import com.lealone.orm.format.UuidFormat;

/**
 * UUID property.
 */
public class PUuid<M extends Model<M>> extends PBaseValueEqual<M, UUID> {

    public PUuid(String name, M model) {
        super(name, model);
    }

    @Override
    protected UuidFormat getValueFormat(JsonFormat format) {
        return format.getUuidFormat();
    }

    @Override
    protected Value createValue(UUID value) {
        return ValueUuid.get(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    @Override
    protected void deserialize(Value v) {
        value = (UUID) ValueUuid.get(v.getBytesNoCopy()).getObject();
    }
}

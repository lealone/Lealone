/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueByte;
import org.lealone.orm.Model;

/**
 * Byte property.
 */
public class PByte<M extends Model<M>> extends PBaseNumber<M, Byte> {

    public PByte(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(Byte value) {
        return ValueByte.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getByte();
    }

    @Override
    protected void deserialize(Object v) {
        value = ((Number) v).byteValue();
    }
}

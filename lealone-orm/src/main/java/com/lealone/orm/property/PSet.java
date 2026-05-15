/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueSet;
import com.lealone.orm.Model;

/**
 * Set property.
 */
public class PSet<M extends Model<M>, E> extends PBase<M, Set<E>> {

    public PSet(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(Set<E> values) {
        return ValueSet.get(values);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void deserialize(Value v) {
        if (v instanceof ValueSet) {
            this.value = (Set<E>) v.getObject();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Set<E> decode(Object v) {
        if (v instanceof List)
            return new HashSet<>((List<E>) v);
        return (Set<E>) v;
    }
}

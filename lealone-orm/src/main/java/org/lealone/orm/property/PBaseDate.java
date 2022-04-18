/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import org.lealone.orm.Model;

/**
 * Base property for date and date time types.
 *
 * @param <M> the type of the owning model
 * @param <D> the date time type
 */
@SuppressWarnings("rawtypes")
public abstract class PBaseDate<M extends Model<M>, D extends Comparable> extends PBaseComparable<M, D> {

    public PBaseDate(String name, M model) {
        super(name, model);
    }

    /**
     * Same as greater than.
     *
     * @param value the equal to bind value
     * @return the model instance
     */
    public M after(D value) {
        return expr().gt(name, value);
    }

    /**
     * Same as less than.
     *
     * @param value the equal to bind value
     * @return the model instance
     */
    public M before(D value) {
        return expr().lt(name, value);
    }
}

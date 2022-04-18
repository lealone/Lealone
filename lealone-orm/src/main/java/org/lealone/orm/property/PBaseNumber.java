/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import org.lealone.orm.Model;

/**
 * Base property for number types.
 *
 * @param <M> the type of the owning model
 * @param <T> the number type
 */
@SuppressWarnings("rawtypes")
public abstract class PBaseNumber<M extends Model<M>, T extends Comparable> extends PBaseComparable<M, T> {

    public PBaseNumber(String name, M model) {
        super(name, model);
    }

    // Additional int versions -- seems the right thing to do

    /**
     * Is equal to.
     *
     * @param value the equal to bind value
     * @return the model instance
     */
    public M eq(int value) {
        return expr().eq(name, value);
    }

    /**
     * Greater than.
     *
     * @param value the equal to bind value
     * @return the model instance
     */
    public M gt(int value) {
        return expr().gt(name, value);
    }

    /**
     * Less than.
     *
     * @param value the equal to bind value
     * @return the model instance
     */
    public M lt(int value) {
        return expr().lt(name, value);
    }

    /**
     * Between lower and upper values.
     *
     * @param lower the lower bind value
     * @param upper the upper bind value
     * @return the model instance
     */
    public M between(int lower, int upper) {
        return expr().between(name, lower, upper);
    }
}

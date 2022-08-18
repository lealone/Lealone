/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import org.lealone.orm.Model;

/**
 * Base property for all comparable types. 
 *
 * @param <M> the type of the owning model
 * @param <T> the type of the scalar property
 */
@SuppressWarnings("rawtypes")
public abstract class PBaseComparable<M extends Model<M>, T extends Comparable>
        extends PBaseValueEqual<M, T> {

    public PBaseComparable(String name, M model) {
        super(name, model);
    }

    // ---- range comparisons -------
    /**
     * Greater than.
     *
     * @param value the bind value
     * @return the model instance
     */
    public final M gt(T value) {
        return expr().gt(name, value);
    }

    /**
     * Greater than or Equal to.
     *
     * @param value the bind value
     * @return the model instance
     */
    public final M ge(T value) {
        return expr().ge(name, value);
    }

    /**
     * Less than.
     *
     * @param value the bind value
     * @return the model instance
     */
    public final M lt(T value) {
        return expr().lt(name, value);
    }

    /**
     * Less than or Equal to.
     *
     * @param value the bind value
     * @return the model instance
     */
    public final M le(T value) {
        return expr().le(name, value);
    }

    /**
     * Between lower and upper values.
     *
     * @param lower the lower bind value
     * @param upper the upper bind value
     * @return the model instance
     */
    public final M between(T lower, T upper) {
        return expr().between(name, lower, upper);
    }
}

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
 * @param <M> the type of the owning model bean
 * @param <T> the number type
 */
@SuppressWarnings("rawtypes")
public abstract class PBaseNumber<M extends Model<M>, T extends Comparable> extends PBaseComparable<M, T> {

    public PBaseNumber(String name, M model) {
        super(name, model);
    }

    private PBaseNumber<M, T> P(M model) {
        return this.<PBaseNumber<M, T>> getModelProperty(model);
    }

    // Additional int versions -- seems the right thing to do

    /**
     * Is equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public M eq(int value) {
        M m = getModel();
        if (m != model) {
            return P(m).eq(value);
        }
        expr().eq(name, value);
        return model;
    }

    /**
     * Greater than.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public M gt(int value) {
        M m = getModel();
        if (m != model) {
            return P(m).gt(value);
        }
        expr().gt(name, value);
        return model;
    }

    /**
     * Less than.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public M lt(int value) {
        M m = getModel();
        if (m != model) {
            return P(m).lt(value);
        }
        expr().lt(name, value);
        return model;
    }

    /**
     * Between lower and upper values.
     *
     * @param lower the lower bind value
     * @param upper the upper bind value
     * @return the root model bean instance
     */
    public M between(int lower, int upper) {
        M m = getModel();
        if (m != model) {
            return P(m).between(lower, upper);
        }
        expr().between(name, lower, upper);
        return model;
    }
}

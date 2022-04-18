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
 * @param <R> the root model bean type
 * @param <T> the number type
 */
@SuppressWarnings("rawtypes")
public abstract class PBaseNumber<R, T extends Comparable> extends PBaseComparable<R, T> {

    public PBaseNumber(String name, R root) {
        super(name, root);
    }

    private PBaseNumber<R, T> P(Model<?> model) {
        return this.<PBaseNumber<R, T>> getModelProperty(model);
    }

    // Additional int versions -- seems the right thing to do

    /**
     * Is equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R eq(int value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).eq(value);
        }
        expr().eq(name, value);
        return root;
    }

    /**
     * Greater than.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R gt(int value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).gt(value);
        }
        expr().gt(name, value);
        return root;
    }

    /**
     * Less than.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R lt(int value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).lt(value);
        }
        expr().lt(name, value);
        return root;
    }

    /**
     * Between lower and upper values.
     *
     * @param lower the lower bind value
     * @param upper the upper bind value
     * @return the root model bean instance
     */
    public R between(int lower, int upper) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).between(lower, upper);
        }
        expr().between(name, lower, upper);
        return root;
    }
}

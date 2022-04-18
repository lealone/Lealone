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
 * @param <M> the type of the owning model bean
 * @param <T> the type of the scalar property
 */
@SuppressWarnings("rawtypes")
public abstract class PBaseComparable<M extends Model<M>, T extends Comparable> extends PBaseValueEqual<M, T> {

    public PBaseComparable(String name, M model) {
        super(name, model);
    }

    private PBaseComparable<M, T> P(M model) {
        return this.<PBaseComparable<M, T>> getModelProperty(model);
    }

    // ---- range comparisons -------
    /**
     * Greater than.
     *
     * @param value the bind value
     * @return the root model bean instance
     */
    public final M gt(T value) {
        M m = getModel();
        if (m != model) {
            return P(m).gt(value);
        }
        expr().gt(name, value);
        return model;
    }

    /**
     * Greater than or Equal to.
     *
     * @param value the bind value
     * @return the root model bean instance
     */
    public final M ge(T value) {
        M m = getModel();
        if (m != model) {
            return P(m).ge(value);
        }
        expr().ge(name, value);
        return model;
    }

    /**
     * Less than.
     *
     * @param value the bind value
     * @return the root model bean instance
     */
    public final M lt(T value) {
        M m = getModel();
        if (m != model) {
            return P(m).lt(value);
        }
        expr().lt(name, value);
        return model;
    }

    /**
     * Less than or Equal to.
     *
     * @param value the bind value
     * @return the root model bean instance
     */
    public final M le(T value) {
        M m = getModel();
        if (m != model) {
            return P(m).le(value);
        }
        expr().le(name, value);
        return model;
    }

    /**
     * Between lower and upper values.
     *
     * @param lower the lower bind value
     * @param upper the upper bind value
     * @return the root model bean instance
     */
    public final M between(T lower, T upper) {
        M m = getModel();
        if (m != model) {
            return P(m).between(lower, upper);
        }
        expr().between(name, lower, upper);
        return model;
    }

}

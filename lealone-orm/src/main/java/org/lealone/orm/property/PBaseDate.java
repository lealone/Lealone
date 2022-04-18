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
 * @param <M> the type of the owning model bean
 * @param <D> the date time type
 */
@SuppressWarnings("rawtypes")
public abstract class PBaseDate<M extends Model<M>, D extends Comparable> extends PBaseComparable<M, D> {

    public PBaseDate(String name, M model) {
        super(name, model);
    }

    private PBaseDate<M, D> P(M model) {
        return this.<PBaseDate<M, D>> getModelProperty(model);
    }

    /**
     * Same as greater than.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public M after(D value) {
        M m = getModel();
        if (m != model) {
            return P(m).after(value);
        }
        expr().gt(name, value);
        return model;
    }

    /**
     * Same as less than.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public M before(D value) {
        M m = getModel();
        if (m != model) {
            return P(m).before(value);
        }
        expr().lt(name, value);
        return model;
    }
}

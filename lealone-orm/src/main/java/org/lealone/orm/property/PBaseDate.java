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
 * @param <R> the root model bean type
 * @param <D> the date time type
 */
@SuppressWarnings("rawtypes")
public abstract class PBaseDate<R, D extends Comparable> extends PBaseComparable<R, D> {

    public PBaseDate(String name, R root) {
        super(name, root);
    }

    private PBaseDate<R, D> P(Model<?> model) {
        return this.<PBaseDate<R, D>> getModelProperty(model);
    }

    /**
     * Same as greater than.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R after(D value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).after(value);
        }
        expr().gt(name, value);
        return root;
    }

    /**
     * Same as less than.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R before(D value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).before(value);
        }
        expr().lt(name, value);
        return root;
    }
}

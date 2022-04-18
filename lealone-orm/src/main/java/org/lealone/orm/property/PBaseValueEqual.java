/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Collection;

import org.lealone.orm.Model;
import org.lealone.orm.ModelProperty;

/**
 * Base property for types that primarily have equal to.
 *
 * @param <M> the type of the owning model bean
 * @param <T> the property type
 */
public abstract class PBaseValueEqual<M extends Model<M>, T> extends ModelProperty<M> {

    public PBaseValueEqual(String name, M model) {
        super(name, model);
    }

    private PBaseValueEqual<M, T> P(M model) {
        return this.<PBaseValueEqual<M, T>> getModelProperty(model);
    }

    /**
     * Is equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public final M eq(T value) {
        M m = getModel();
        if (m != model) {
            return P(m).eq(value);
        }
        expr().eq(name, value);
        return model;
    }

    /**
     * Is not equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public final M ne(T value) {
        M m = getModel();
        if (m != model) {
            return P(m).ne(value);
        }
        expr().ne(name, value);
        return model;
    }

    /**
     * Is in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the root model bean instance
     */
    @SafeVarargs
    public final M in(T... values) {
        M m = getModel();
        if (m != model) {
            return P(m).in(values);
        }
        expr().in(name, (Object[]) values);
        return model;
    }

    /**
     * Is NOT in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the root model bean instance
     */
    @SafeVarargs
    public final M notIn(T... values) {
        M m = getModel();
        if (m != model) {
            return P(m).notIn(values);
        }
        expr().notIn(name, (Object[]) values);
        return model;
    }

    /**
     * Is in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the root model bean instance
     */
    public final M in(Collection<T> values) {
        M m = getModel();
        if (m != model) {
            return P(m).in(values);
        }
        expr().in(name, values);
        return model;
    }

    /**
     * Is NOT in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the root model bean instance
     */
    public final M notIn(Collection<T> values) {
        M m = getModel();
        if (m != model) {
            return P(m).notIn(values);
        }
        expr().notIn(name, values);
        return model;
    }

}

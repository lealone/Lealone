/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Collection;

import org.lealone.orm.Model;

/**
 * Base property for types that primarily have equal to.
 *
 * @param <M> the type of the owning model bean
 * @param <T> the property type
 */
public abstract class PBaseValueEqual<M extends Model<M>, T> extends PBase<M, T> {

    public PBaseValueEqual(String name, M model) {
        super(name, model);
    }

    /**
     * Is equal to.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public final M eq(T value) {
        return expr().eq(name, value);
    }

    /**
     * Is not equal to.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public final M ne(T value) {
        return expr().ne(name, value);
    }

    /**
     * Is in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the model bean instance
     */
    @SafeVarargs
    public final M in(T... values) {
        return expr().in(name, (Object[]) values);
    }

    /**
     * Is NOT in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the model bean instance
     */
    @SafeVarargs
    public final M notIn(T... values) {
        return expr().notIn(name, (Object[]) values);
    }

    /**
     * Is in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the model bean instance
     */
    public final M in(Collection<T> values) {
        return expr().in(name, values);
    }

    /**
     * Is NOT in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the model bean instance
     */
    public final M notIn(Collection<T> values) {
        return expr().notIn(name, values);
    }
}

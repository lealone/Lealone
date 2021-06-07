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
 * @param <R> the root model bean type
 * @param <T> the property type
 */
public abstract class PBaseValueEqual<R, T> extends ModelProperty<R> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PBaseValueEqual(String name, R root) {
        super(name, root);
    }

    private PBaseValueEqual<R, T> P(Model<?> model) {
        return this.<PBaseValueEqual<R, T>> getModelProperty(model);
    }

    /**
     * Is equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public final R eq(T value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).eq(value);
        }
        expr().eq(name, value);
        return root;
    }

    /**
     * Is not equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public final R ne(T value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).ne(value);
        }
        expr().ne(name, value);
        return root;
    }

    /**
     * Is in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the root model bean instance
     */
    @SafeVarargs
    public final R in(T... values) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).in(values);
        }
        expr().in(name, (Object[]) values);
        return root;
    }

    /**
     * Is NOT in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the root model bean instance
     */
    @SafeVarargs
    public final R notIn(T... values) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).notIn(values);
        }
        expr().notIn(name, (Object[]) values);
        return root;
    }

    /**
     * Is in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the root model bean instance
     */
    public final R in(Collection<T> values) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).in(values);
        }
        expr().in(name, values);
        return root;
    }

    /**
     * Is NOT in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the root model bean instance
     */
    public final R notIn(Collection<T> values) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).notIn(values);
        }
        expr().notIn(name, values);
        return root;
    }

}

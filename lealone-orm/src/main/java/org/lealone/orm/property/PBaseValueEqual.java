/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.orm.property;

import java.util.Collection;

import org.lealone.orm.ModelProperty;

/**
 * Base property for types that primarily have equal to.
 *
 * @param <R> the root model bean type
 * @param <T> the number type
 */
public abstract class PBaseValueEqual<R, T, P> extends ModelProperty<R, P> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PBaseValueEqual(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PBaseValueEqual(String name, R root, String prefix) {
        super(name, root, prefix);
    }

    /**
     * Is equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public final R equalTo(T value) {
        expr().eq(name, value);
        return root;
    }

    /**
     * Is equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public final R eq(T value) {
        expr().eq(name, value);
        return root;
    }

    /**
     * Is not equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public final R notEqualTo(T value) {
        expr().ne(name, value);
        return root;
    }

    /**
     * Is not equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public final R ne(T value) {
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
        expr().notIn(name, (Object[]) values);
        return root;
    }

    /**
     * Is in a list of values. Synonym for in().
     *
     * @param values the list of values for the predicate
     * @return the root model bean instance
     */
    @SafeVarargs
    public final R isIn(T... values) {
        expr().in(name, (Object[]) values);
        return root;
    }

    /**
     * Is in a list of values.
     *
     * @param values the list of values for the predicate
     * @return the root model bean instance
     */
    public final R in(Collection<T> values) {
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
        expr().notIn(name, values);
        return root;
    }

    /**
     * Is in a list of values. Synonym for in().
     *
     * @param values the list of values for the predicate
     * @return the root model bean instance
     */
    public final R isIn(Collection<T> values) {
        expr().in(name, values);
        return root;
    }

}

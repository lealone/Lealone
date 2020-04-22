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

import org.lealone.orm.Model;

/**
 * Base property for number types.
 *
 * @param <R> the root model bean type
 * @param <T> the number type
 */
@SuppressWarnings("rawtypes")
public abstract class PBaseNumber<R, T extends Comparable> extends PBaseComparable<R, T> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
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

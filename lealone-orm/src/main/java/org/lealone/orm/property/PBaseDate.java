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
 * Base property for date and date time types.
 *
 * @param <R> the root model bean type
 * @param <D> the date time type
 */
@SuppressWarnings("rawtypes")
public abstract class PBaseDate<R, D extends Comparable> extends PBaseComparable<R, D> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
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

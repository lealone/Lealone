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
package org.lealone.orm.typequery;

import org.lealone.orm.ExpressionList;
import org.lealone.orm.Query;

/**
 * A property used in type query.
 *
 * @param <R> The type of the owning root bean
 */
public class TQProperty<R> {

    protected final String name;

    protected final R root;

    /**
     * Construct with a property name and root instance.
     *
     * @param name the name of the property
     * @param root the root query bean instance
     */
    public TQProperty(String name, R root) {
        this(name, root, null);
    }

    /**
     * Construct with additional path prefix.
     */
    public TQProperty(String name, R root, String prefix) {
        this.root = root;
        name = fullPath(prefix, name);

        if (((Query<?, ?>) root).databaseToUpper()) {
            name = name.toUpperCase();
        }
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Internal method to return the underlying expression list.
     */
    protected ExpressionList<?> expr() {
        return ((Query<?, ?>) root).peekExprList();
    }

    /**
     * Is null.
     */
    public R isNull() {
        expr().isNull(name);
        return root;
    }

    /**
     * Is not null.
     */
    public R isNotNull() {
        expr().isNotNull(name);
        return root;
    }

    /**
     * Order by ascending on this property.
     */
    public R asc() {
        expr().orderBy().asc(name);
        return root;
    }

    /**
     * Order by descending on this property.
     */
    public R desc() {
        expr().orderBy().desc(name);
        return root;
    }

    /**
     * Return the property name.
     */
    public String propertyName() {
        return name;
    }

    /**
     * Return the full path by adding the prefix to the property name (null safe).
     */
    public static String fullPath(String prefix, String name) {
        return (prefix == null) ? name : prefix + "." + name;
    }
}

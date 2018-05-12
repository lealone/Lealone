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

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.util.HashMap;

import org.lealone.db.value.Value;
import org.lealone.orm.ExpressionList;
import org.lealone.orm.Query;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A property used in type query.
 *
 * @param <R> The type of the owning root bean
 */
public class TQProperty<R> {

    protected final String name;

    protected final R root;

    protected boolean changed;

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

        if (((Query<?>) root).databaseToUpper()) {
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
        return ((Query<?>) root).peekExprList();
    }

    protected boolean isReady() {
        return ((Query<?>) root).isReady();
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

    public R serialize(JsonGenerator jgen) throws IOException {
        return root;
    }

    public R deserialize(JsonNode node) {
        return root;
    }

    public R deserialize(HashMap<String, Value> map) {
        return root;
    }

    /**
     * Helper method to check if two objects are equal.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected boolean areEqual(Object obj1, Object obj2) {
        if (obj1 == null) {
            return (obj2 == null);
        }
        if (obj2 == null) {
            return false;
        }
        if (obj1 == obj2) {
            return true;
        }
        if (obj1 instanceof BigDecimal) {
            // Use comparable for BigDecimal as equals
            // uses scale in comparison...
            if (obj2 instanceof BigDecimal) {
                Comparable com1 = (Comparable) obj1;
                return (com1.compareTo(obj2) == 0);
            } else {
                return false;
            }
        }
        if (obj1 instanceof URL) {
            // use the string format to determine if dirty
            return obj1.toString().equals(obj2.toString());
        }
        return obj1.equals(obj2);
    }

    /**
     * Return the full path by adding the prefix to the property name (null safe).
     */
    public static String fullPath(String prefix, String name) {
        return (prefix == null) ? name : prefix + "." + name;
    }
}

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
package org.lealone.orm;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.util.HashMap;

import org.lealone.db.value.Value;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A property used in type query.
 *
 * @param <R> The type of the owning root bean
 */
public abstract class ModelProperty<R> {

    protected final String name;
    protected final R root;

    /**
     * Construct with a property name and root instance.
     *
     * @param name the name of the property
     * @param root the root model bean instance
     */
    public ModelProperty(String name, R root) {
        this(name, root, null);
    }

    /**
     * Construct with additional path prefix.
     */
    public ModelProperty(String name, R root, String prefix) {
        this.root = root;
        this.name = fullPath(prefix, name);
    }

    public String getDatabaseName() {
        return getModel().getDatabaseName();
    }

    public String getSchemaName() {
        return getModel().getSchemaName();
    }

    public String getTableName() {
        return getModel().getTableName();
    }

    @Override
    public String toString() {
        return name;
    }

    private Model<?> getModel() {
        return ((Model<?>) root);
    }

    /**
     * Internal method to return the underlying expression builder.
     */
    protected ExpressionBuilder<?> expr() {
        return ((Model<?>) root).peekExprBuilder();
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
        expr().orderBy(name, false);
        return root;
    }

    /**
     * Order by descending on this property.
     */
    public R desc() {
        expr().orderBy(name, true);
        return root;
    }

    /**
     * Return the property name.
     */
    public String getName() {
        return name;
    }

    public final R eq(ModelProperty<?> p) {
        expr().eq(name, p);
        return root;
    }

    // public abstract R set(Object value);

    public R set(Object value) {
        return root;
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

    protected JsonNode getJsonNode(JsonNode node) {
        JsonNode n = node.get(name);
        if (n == null) {
            n = node.get(name.toLowerCase());
        }
        return n;
    }

    /**
     * Helper method to check if two objects are equal.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static boolean areEqual(Object obj1, Object obj2) {
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
    protected static String fullPath(String prefix, String name) {
        return (prefix == null) ? name : prefix + "." + name;
    }
}

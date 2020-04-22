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
@SuppressWarnings("unchecked")
public abstract class ModelProperty<R> {

    protected final String name;
    protected final R root;

    protected String fullName;

    /**
     * Construct with a property name and root instance.
     *
     * @param name the name of the property
     * @param root the root model bean instance
     */
    public ModelProperty(String name, R root) {
        this.name = name;
        this.root = root;
    }

    public String getDatabaseName() {
        return ((Model<?>) root).getDatabaseName();
    }

    public String getSchemaName() {
        return ((Model<?>) root).getSchemaName();
    }

    public String getTableName() {
        return ((Model<?>) root).getTableName();
    }

    @Override
    public String toString() {
        return name;
    }

    protected Model<?> getModel() {
        return ((Model<?>) root).maybeCopy();
    }

    protected <P> P getModelProperty(Model<?> model) {
        return (P) model.getModelProperty(name);
    }

    private ModelProperty<R> P(Model<?> model) {
        return this.<ModelProperty<R>> getModelProperty(model);
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
        Model<?> model = getModel();
        if (model != root) {
            return P(model).isNull();
        }
        expr().isNull(name);
        return root;
    }

    /**
     * Is not null.
     */
    public R isNotNull() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).isNotNull();
        }
        expr().isNotNull(name);
        return root;
    }

    /**
     * Order by ascending on this property.
     */
    public R asc() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).asc();
        }
        expr().orderBy(name, false);
        return root;
    }

    /**
     * Order by descending on this property.
     */
    public R desc() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).desc();
        }
        expr().orderBy(name, true);
        return root;
    }

    /**
     * Return the property name.
     */
    public String getName() {
        return name;
    }

    protected String getFullName() {
        if (fullName == null) {
            fullName = getSchemaName() + "." + getTableName() + "." + name;
        }
        return fullName;
    }

    public final R eq(ModelProperty<?> p) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).eq(p);
        }
        expr().eq(name, p);
        return root;
    }

    public R set(Object value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
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
    @SuppressWarnings({ "rawtypes" })
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
}

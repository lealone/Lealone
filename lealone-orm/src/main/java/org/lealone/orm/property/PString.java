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

import java.io.IOException;
import java.util.HashMap;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.orm.Model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * String property.
 *
 * @param <R> the root model bean type
 */
public class PString<R> extends PBaseComparable<R, String> {

    private String value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PString(String name, R root) {
        super(name, root);
    }

    private PString<R> P(Model<?> model) {
        return this.<PString<R>> getModelProperty(model);
    }

    /**
     * Case insensitive is equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R ieq(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).ieq(value);
        }
        expr().ieq(name, value);
        return root;
    }

    /**
     * Case insensitive is equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R iequalTo(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).iequalTo(value);
        }
        expr().ieq(name, value);
        return root;
    }

    /**
     * Like - include '%' and '_' placeholders as necessary.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R like(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).like(value);
        }
        expr().like(name, value);
        return root;
    }

    /**
     * Starts with - uses a like with '%' wildcard added to the end.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R startsWith(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).startsWith(value);
        }
        expr().startsWith(name, value);
        return root;
    }

    /**
     * Ends with - uses a like with '%' wildcard added to the beginning.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R endsWith(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).endsWith(value);
        }
        expr().endsWith(name, value);
        return root;
    }

    /**
     * Contains - uses a like with '%' wildcard added to the beginning and end.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R contains(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).contains(value);
        }
        expr().contains(name, value);
        return root;
    }

    /**
     * Case insensitive like.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R ilike(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).ilike(value);
        }
        expr().ilike(name, value);
        return root;
    }

    /**
     * Case insensitive starts with.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R istartsWith(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).istartsWith(value);
        }
        expr().istartsWith(name, value);
        return root;
    }

    /**
     * Case insensitive ends with.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R iendsWith(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).iendsWith(value);
        }
        expr().iendsWith(name, value);
        return root;
    }

    /**
     * Case insensitive contains.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R icontains(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).icontains(value);
        }
        expr().icontains(name, value);
        return root;
    }

    /**
     * Add a full text "Match" expression.
     * <p>
     * This means the query will automatically execute against the document store (ElasticSearch).
     * </p>
     *
     * @param value the match expression
     */
    public R match(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).match(value);
        }
        expr().match(name, value);
        return root;
    }

    public final R set(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueString.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(value.toString());
    }

    public final String get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
        }
        return value;
    }

    @Override
    public R serialize(JsonGenerator jgen) throws IOException {
        jgen.writeStringField(getName(), value);
        return root;
    }

    @Override
    public R deserialize(JsonNode node) {
        node = getJsonNode(node);
        if (node == null) {
            return root;
        }
        set(node.asText());
        return root;
    }

    @Override
    public R deserialize(HashMap<String, Value> map) {
        Value v = map.get(getFullName());
        if (v != null) {
            value = v.getString();
        }
        return root;
    }

}

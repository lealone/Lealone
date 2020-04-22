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
import org.lealone.db.value.ValueBoolean;
import org.lealone.orm.Model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;

/**
 * Boolean property.
 *
 * @param <R> the root model bean type
 */
public class PBoolean<R> extends PBaseValueEqual<R, Boolean> {

    private boolean value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PBoolean(String name, R root) {
        super(name, root);
    }

    private PBoolean<R> P(Model<?> model) {
        return this.<PBoolean<R>> getModelProperty(model);
    }

    /**
     * Is true.
     *
     * @return the root model bean instance
     */
    public R isTrue() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).isTrue();
        }
        expr().eq(name, Boolean.TRUE);
        return root;
    }

    /**
     * Is false.
     *
     * @return the root model bean instance
     */
    public R isFalse() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).isFalse();
        }
        expr().eq(name, Boolean.FALSE);
        return root;
    }

    /**
     * Is true or false based on the bind value.
     *
     * @param value the equal to bind value
     *
     * @return the root model bean instance
     */
    public R is(boolean value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).is(value);
        }
        expr().eq(name, value);
        return root;
    }

    /**
     * Is true or false based on the bind value.
     *
     * @param value the equal to bind value
     *
     * @return the root model bean instance
     */
    public R eq(boolean value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).eq(value);
        }
        expr().eq(name, value);
        return root;
    }

    public final R set(boolean value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueBoolean.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Boolean.valueOf(value.toString()).booleanValue());
    }

    public final boolean get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
        }
        return value;
    }

    @Override
    public R serialize(JsonGenerator jgen) throws IOException {
        jgen.writeBooleanField(getName(), value);
        return root;
    }

    @Override
    public R deserialize(JsonNode node) {
        node = getJsonNode(node);
        if (node == null) {
            return root;
        }
        set(((BooleanNode) node).booleanValue());
        return root;
    }

    @Override
    public R deserialize(HashMap<String, Value> map) {
        Value v = map.get(getFullName());
        if (v != null) {
            value = v.getBoolean();
        }
        return root;
    }
}

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
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueString;
import org.lealone.orm.Model;
import org.lealone.orm.ModelProperty;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Array property with E as the element type.
 *
 * @param <R> the root model bean type
 */
public class PArray<R> extends ModelProperty<R> {

    private Object[] values;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PArray(String name, R root) {
        super(name, root);
    }

    private PArray<R> P(Model<?> model) {
        return this.<PArray<R>> getModelProperty(model);
    }

    public final R set(Object[] values) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(values);
        }
        if (!areEqual(this.values, values)) {
            this.values = values;
            Value[] array = new Value[values.length];
            for (int i = 0; i < values.length; i++) {
                array[i] = ValueString.get(values[i].toString());
            }
            expr().set(name, ValueArray.get(array));
        }
        return root;
    }

    public final Object[] get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
        }
        return values;
    }

    @Override
    public R serialize(JsonGenerator jgen) throws IOException {
        jgen.writeFieldName(getName());
        jgen.writeStartArray();
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                jgen.writeObject(values[i]);
            }
        }
        jgen.writeEndArray();
        return root;
    }

    @Override
    public R deserialize(JsonNode node) {
        node = getJsonNode(node);
        if (node == null) {
            return root;
        }
        ArrayNode arrayNode = (ArrayNode) node;
        int length = arrayNode.size();
        if (length > 0) {
            Object[] values = new Object[length];
            for (int i = 0; i < length; i++) {
                values[i] = arrayNode.get(i);
            }
            set(values);
        }
        return root;
    }

    @Override
    public R deserialize(HashMap<String, Value> map) {
        Value v = map.get(getFullName());
        if (v != null && (v instanceof ValueArray)) {
            ValueArray array = (ValueArray) v;
            Value[] list = array.getList();
            int length = list.length;
            Object[] values = new Object[length];
            for (int i = 0; i < length; i++) {
                values[i] = list[i].getObject();
            }
            this.values = values;
        }
        return root;
    }

    /**
     * ARRAY contains the values.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.contains("4321")
     *    .findList();
     *
     * }</pre>
     *
     * @param values The values that should be contained in the array
     */
    @SafeVarargs
    public final R contains(Object... values) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).contains(values);
        }
        expr().arrayContains(name, values);
        return root;
    }

    /**
     * ARRAY does not contain the values.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.notContains("4321")
     *    .findList();
     *
     * }</pre>
     *
     * @param values The values that should not be contained in the array
     */
    @SafeVarargs
    public final R notContains(Object... values) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).notContains(values);
        }
        expr().arrayNotContains(name, values);
        return root;
    }

    /**
     * ARRAY is empty.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.isEmpty()
     *    .findList();
     *
     * }</pre>
     */
    public R isEmpty() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).isEmpty();
        }
        expr().arrayIsEmpty(name);
        return root;
    }

    /**
     * ARRAY is not empty.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.isNotEmpty()
     *    .findList();
     *
     * }</pre>
     */
    public R isNotEmpty() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).isNotEmpty();
        }
        expr().arrayIsNotEmpty(name);
        return root;
    }

}

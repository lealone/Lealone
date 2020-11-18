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

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueJavaObject;
import org.lealone.orm.Model;
import org.lealone.orm.ModelProperty;
import org.lealone.orm.json.JsonObject;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

public class PObject<R> extends ModelProperty<R> {

    private Object value;

    public PObject(String name, R root) {
        super(name, root);
    }

    private PObject<R> P(Model<?> model) {
        return this.<PObject<R>> getModelProperty(model);
    }

    @Override
    public R set(Object value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueJavaObject.getNoCopy(value, null));
        }
        return root;
    }

    public final Object get() {
        return value;
    }

    @Override
    public R serialize(JsonGenerator jgen) throws IOException {
        JsonObject json = JsonObject.mapFrom(value);
        String str = json.encode();
        jgen.writeStringField(getName(), value.getClass().getName() + "," + str);
        return root;
    }

    @Override
    public R deserialize(JsonNode node) {
        node = getJsonNode(node);
        if (node == null) {
            return root;
        }
        String text = node.asText();
        int pos = text.indexOf(',');
        String className = text.substring(0, pos);
        String json = text.substring(pos + 1);
        try {
            Object obj = new JsonObject(json).mapTo(Class.forName(className));
            set(obj);
        } catch (ClassNotFoundException e) {
            throw DbException.convert(e);
        }

        return root;
    }

    @Override
    public R deserialize(HashMap<String, Value> map) {
        Value v = map.get(getFullName());
        if (v != null) {
            value = v.getObject();
        }
        return root;
    }
}

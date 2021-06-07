/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.io.IOException;

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
        jgen.writeStringField(getName(), value == null ? null : value.getClass().getName() + "," + str);
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
    protected void deserialize(Value v) {
        value = v.getObject();
    }
}

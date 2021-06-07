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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * byte[] property.
 *
 * @param <R> the root model bean type
 */
public class PBytes<R> extends ModelProperty<R> {

    private byte[] value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PBytes(String name, R root) {
        super(name, root);
    }

    private PBytes<R> P(Model<?> model) {
        return this.<PBytes<R>> getModelProperty(model);
    }

    public R set(byte[] value) {
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

    public final byte[] get() {
        return value;
    }

    @Override
    public R serialize(JsonGenerator jgen) throws IOException {
        jgen.writeBinaryField(getName(), value);
        return root;
    }

    @Override
    public R deserialize(JsonNode node) {
        node = getJsonNode(node);
        if (node == null) {
            return root;
        }

        try {
            set(node.binaryValue());
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        return root;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBytes();
    }
}

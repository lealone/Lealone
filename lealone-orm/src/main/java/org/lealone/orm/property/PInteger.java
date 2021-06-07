/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.io.IOException;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.orm.Model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;

/**
 * Integer property.
 *
 * @param <R> the root model bean type
 */
public class PInteger<R> extends PBaseNumber<R, Integer> {

    private int value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PInteger(String name, R root) {
        super(name, root);
    }

    private PInteger<R> P(Model<?> model) {
        return this.<PInteger<R>> getModelProperty(model);
    }

    public final R set(int value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueInt.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Integer.valueOf(value.toString()).intValue());
    }

    public final int get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
        }
        return value;
    }

    @Override
    public R serialize(JsonGenerator jgen) throws IOException {
        jgen.writeNumberField(getName(), value);
        return root;
    }

    @Override
    public R deserialize(JsonNode node) {
        node = getJsonNode(node);
        if (node == null) {
            return root;
        }
        set(((NumericNode) node).asInt());
        return root;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getInt();
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.io.IOException;
import java.sql.Timestamp;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueTimestamp;
import org.lealone.orm.Model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;

/**
 * Property for java sql Timestamp.
 *
 * @param <R> the root model bean type
 */
public class PTimestamp<R> extends PBaseDate<R, Timestamp> {

    private Timestamp value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PTimestamp(String name, R root) {
        super(name, root);
    }

    private PTimestamp<R> P(Model<?> model) {
        return this.<PTimestamp<R>> getModelProperty(model);
    }

    public final R set(Timestamp value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueTimestamp.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Timestamp.valueOf(value.toString()));
    }

    public final Timestamp get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
        }
        return value;
    }

    @Override
    public R serialize(JsonGenerator jgen) throws IOException {
        jgen.writeNumberField(getName(), value == null ? 0 : value.getTime());
        return root;
    }

    @Override
    public R deserialize(JsonNode node) {
        node = getJsonNode(node);
        if (node == null) {
            return root;
        }
        Timestamp t = new Timestamp(((NumericNode) node).asLong());
        set(t);
        return root;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getTimestamp();
    }
}

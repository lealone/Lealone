/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.io.IOException;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLong;
import org.lealone.orm.Model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;

/**
 * Long property.
 *
 * @param <R> the root model bean type
 */
public class PLong<R> extends PBaseNumber<R, Long> {

    private long value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PLong(String name, R root) {
        super(name, root);
    }

    private PLong<R> P(Model<?> model) {
        return this.<PLong<R>> getModelProperty(model);
    }

    public R set(long value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueLong.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Long.valueOf(value.toString()).longValue());
    }

    public final long get() {
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
        set(((NumericNode) node).asLong());
        return root;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getLong();
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.io.IOException;
import java.sql.Time;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueTime;
import org.lealone.orm.Model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;

/**
 * Time property.
 *
 * @param <R> the root model bean type
 */
public class PTime<R> extends PBaseNumber<R, Time> {

    private Time value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PTime(String name, R root) {
        super(name, root);
    }

    private PTime<R> P(Model<?> model) {
        return this.<PTime<R>> getModelProperty(model);
    }

    public final R set(Time value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueTime.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Time.valueOf(value.toString()));
    }

    public final Time get() {
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
        Time t = new Time(((NumericNode) node).asLong());
        set(t);
        return root;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getTime();
    }
}

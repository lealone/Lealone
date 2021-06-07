/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.io.IOException;
import java.math.BigDecimal;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDecimal;
import org.lealone.orm.Model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DecimalNode;

/**
 * BigDecimal property.
 * @param <R> the root model bean type
 */
public class PBigDecimal<R> extends PBaseNumber<R, BigDecimal> {

    private BigDecimal value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PBigDecimal(String name, R root) {
        super(name, root);
    }

    private PBigDecimal<R> P(Model<?> model) {
        return this.<PBigDecimal<R>> getModelProperty(model);
    }

    public final R set(BigDecimal value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueDecimal.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(new BigDecimal(value.toString()));
    }

    public final BigDecimal get() {
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
        set(((DecimalNode) node).decimalValue());
        return root;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBigDecimal();
    }
}

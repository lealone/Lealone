/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.io.IOException;
import java.sql.Date;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDate;
import org.lealone.orm.Model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;

/**
 * Java sql date property.
 *
 * @param <R> the root model bean type
 */
public class PDate<R> extends PBaseDate<R, Date> {

    private Date value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PDate(String name, R root) {
        super(name, root);
    }

    private PDate<R> P(Model<?> model) {
        return this.<PDate<R>> getModelProperty(model);
    }

    public final R set(Date value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueDate.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Date.valueOf(value.toString()));
    }

    public final Date get() {
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
        Date date = new Date(((NumericNode) node).asLong());
        set(date);
        return root;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getDate();
    }
}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.io.IOException;
import java.util.UUID;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueUuid;
import org.lealone.orm.Model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * UUID property.
 *
 * @param <R> the root model bean type
 */
public class PUuid<R> extends PBaseValueEqual<R, UUID> {

    private UUID value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PUuid(String name, R root) {
        super(name, root);
    }

    private PUuid<R> P(Model<?> model) {
        return this.<PUuid<R>> getModelProperty(model);
    }

    public R set(UUID value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueUuid.get(value.getMostSignificantBits(), value.getLeastSignificantBits()));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(UUID.fromString(value.toString()));
    }

    public final UUID get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
        }
        return value;
    }

    @Override
    public R serialize(JsonGenerator jgen) throws IOException {
        jgen.writeStringField(getName(), value == null ? null : value.toString());
        return root;
    }

    @Override
    public R deserialize(JsonNode node) {
        node = getJsonNode(node);
        if (node == null) {
            return root;
        }
        String text = node.asText();
        set(UUID.fromString(text));
        return root;
    }

    @Override
    protected void deserialize(Value v) {
        value = (UUID) ValueUuid.get(v.getBytesNoCopy()).getObject();
    }
}

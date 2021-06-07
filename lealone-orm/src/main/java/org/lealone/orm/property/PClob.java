/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.io.IOException;
import java.sql.Clob;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.ReadonlyClob;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBytes;
import org.lealone.db.value.ValueJavaObject;
import org.lealone.orm.Model;
import org.lealone.orm.ModelProperty;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

public class PClob<R> extends ModelProperty<R> {

    private Clob value;

    public PClob(String name, R root) {
        super(name, root);
    }

    private PClob<R> P(Model<?> model) {
        return this.<PClob<R>> getModelProperty(model);
    }

    public R set(Clob value) {
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

    public final Clob get() {
        return value;
    }

    @Override
    public R serialize(JsonGenerator jgen) throws IOException {
        try {
            jgen.writeFieldName(getName());
            jgen.writeBinary(value.getAsciiStream(), (int) value.length());
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        return root;
    }

    @Override
    public R deserialize(JsonNode node) {
        node = getJsonNode(node);
        if (node == null) {
            return root;
        }
        try {
            byte[] bytes = node.binaryValue();
            ReadonlyClob c = new ReadonlyClob(ValueBytes.get(bytes));
            set(c);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        return root;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getClob();
    }
}

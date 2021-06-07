/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.ReadonlyBlob;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBytes;
import org.lealone.db.value.ValueJavaObject;
import org.lealone.orm.Model;
import org.lealone.orm.ModelProperty;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

public class PBlob<R> extends ModelProperty<R> {

    private Blob value;

    public PBlob(String name, R root) {
        super(name, root);
    }

    private PBlob<R> P(Model<?> model) {
        return this.<PBlob<R>> getModelProperty(model);
    }

    public R set(Blob value) {
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

    public final Blob get() {
        return value;
    }

    @Override
    public R serialize(JsonGenerator jgen) throws IOException {
        try {
            jgen.writeFieldName(getName());
            jgen.writeBinary(value.getBinaryStream(), (int) value.length());
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
            ReadonlyBlob b = new ReadonlyBlob(ValueBytes.get(bytes));
            set(b);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        return root;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBlob();
    }
}

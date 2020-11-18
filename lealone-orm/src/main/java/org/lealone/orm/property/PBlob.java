/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

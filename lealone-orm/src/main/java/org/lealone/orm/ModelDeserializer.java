/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

public abstract class ModelDeserializer<T> extends JsonDeserializer<T> {
    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        Model<T> m = newModelInstance();
        m.deserialize(node);

        JsonNode n = node.get("modelType");
        if (n == null) {
            m.modelType = Model.REGULAR_MODEL;
            // 如果不通过JsonSerializer得到的json串不一定包含isDao字段(比如前端直接传来的json串)，所以不抛异常
            // DbException.throwInternalError("The isDao field is missing");
        } else {
            m.modelType = n.shortValue();
        }
        return (T) m;
    }

    protected abstract Model<T> newModelInstance();
}

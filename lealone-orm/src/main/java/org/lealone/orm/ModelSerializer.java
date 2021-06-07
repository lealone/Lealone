/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

@SuppressWarnings("rawtypes")
public class ModelSerializer<T extends Model> extends JsonSerializer<T> {
    @Override
    public void serialize(T value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonProcessingException {
        jgen.writeStartObject();
        // for (ModelProperty p : ((Model) value).modelProperties) {
        // p.serialize(jgen);
        // }
        // jgen.writeNumberField("modelType", ((Model) value).modelType);

        ((Model) value).serialize(jgen);
        jgen.writeEndObject();
    }
}

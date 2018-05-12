package org.lealone.orm;

import java.io.IOException;

import org.lealone.orm.typequery.TQProperty;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

@SuppressWarnings("rawtypes")
public class QuerySerializer<T extends Query> extends JsonSerializer<T> {
    @Override
    public void serialize(T value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonProcessingException {
        jgen.writeStartObject();
        for (TQProperty p : ((Query) value).tqProperties) {
            p.serialize(jgen);
        }
        jgen.writeEndObject();
    }
}

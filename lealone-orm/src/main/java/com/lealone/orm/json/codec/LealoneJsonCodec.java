/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.json.codec;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.lealone.orm.Model;
import com.lealone.orm.json.Json;
import com.lealone.orm.json.JsonArray;
import com.lealone.orm.json.JsonObject;
import com.lealone.orm.json.JsonParser;

public class LealoneJsonCodec implements JsonCodec {

    @Override
    public String encode(Object object, boolean pretty) {
        LealoneJsonGenerator generator = new LealoneJsonGenerator();
        encodeJson(object, generator);
        return generator.toString();
    }

    @SuppressWarnings("rawtypes")
    private static void encodeJson(Object json, LealoneJsonGenerator generator) {
        if (json instanceof JsonObject) {
            json = ((JsonObject) json).getMap();
        } else if (json instanceof JsonArray) {
            json = ((JsonArray) json).getList();
        }
        if (json instanceof Map) {
            generator.writeStartObject();
            boolean first = true;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) json).entrySet()) {
                if (first)
                    first = false;
                else
                    generator.writeSeparatorChar();
                generator.writeFieldName(e.getKey().toString());
                encodeJson(e.getValue(), generator);
            }
            generator.writeEndObject();
        } else if (json instanceof List) {
            generator.writeStartArray();
            boolean first = true;
            for (Object item : (List<?>) json) {
                if (first)
                    first = false;
                else
                    generator.writeSeparatorChar();
                encodeJson(item, generator);
            }
            generator.writeEndArray();
        } else if (json instanceof Set) {
            generator.writeStartArray();
            boolean first = true;
            for (Object item : (Set<?>) json) {
                if (first)
                    first = false;
                else
                    generator.writeSeparatorChar();
                encodeJson(item, generator);
            }
            generator.writeEndArray();
        } else if (json instanceof CharSequence) {
            generator.writeString(((CharSequence) json).toString());
        } else if (json instanceof Number) {
            if (json instanceof Short) {
                generator.writeNumber((Short) json);
            } else if (json instanceof Integer) {
                generator.writeNumber((Integer) json);
            } else if (json instanceof Long) {
                generator.writeNumber((Long) json);
            } else if (json instanceof Float) {
                generator.writeNumber((Float) json);
            } else if (json instanceof Double) {
                generator.writeNumber((Double) json);
            } else if (json instanceof Byte) {
                generator.writeNumber((Byte) json);
            } else if (json instanceof BigInteger) {
                generator.writeNumber((BigInteger) json);
            } else if (json instanceof BigDecimal) {
                generator.writeNumber((BigDecimal) json);
            } else {
                throw new UnsupportedOperationException();
            }
        } else if (json instanceof Boolean) {
            generator.writeBoolean((Boolean) json);
        } else if (json instanceof Instant) {
            // RFC-7493
            generator.writeString((ISO_INSTANT.format((Instant) json)));
        } else if (json instanceof byte[]) {
            // RFC-7493
            generator.writeString(Json.BASE64_ENCODER.encodeToString((byte[]) json));
        } else if (json instanceof Enum) {
            // vert.x extra (non standard but allowed conversion)
            generator.writeString(((Enum<?>) json).name());
        } else if (json instanceof Model) {
            // 需要转成Map，不能直接用encode，否则嵌套model编码为json时前端不能识别
            encodeJson(((Model) json).toMap(), generator);
        } else if (json == null) {
            generator.writeNull();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Map<String, Object> decodeJsonObject(String json) {
        return new JsonParser().parseJsonObject(json);
    }

    @Override
    public List<Object> decodeJsonArray(String json) {
        return new JsonParser().parseJsonArray(json);
    }

    @Override
    public Object decodeAny(String json) {
        return new JsonParser().parseJsonAny(json);
    }
}

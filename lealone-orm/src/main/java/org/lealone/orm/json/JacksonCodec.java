/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package org.lealone.orm.json;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.lealone.orm.Model;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 * @author zhh
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class JacksonCodec {

    private static final JsonFactory factory = new JsonFactory();

    static {
        // Non-standard JSON but we allow C style comments in our JSON
        factory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    }

    public static String encode(Object object, boolean pretty) throws EncodeException {
        StringWriter sw = new StringWriter();
        JsonGenerator generator = null;
        try {
            generator = factory.createGenerator(sw);
            if (pretty) {
                generator.useDefaultPrettyPrinter();
            }
            encodeJson(object, generator);
            generator.flush();
            return sw.toString();
        } catch (IOException e) {
            throw new DecodeException("Failed to decode:" + e.getMessage(), e);
        } finally {
            close(generator);
        }
    }

    // In recursive calls, the callee is in charge of opening and closing the data structure
    private static void encodeJson(Object json, JsonGenerator generator) throws IOException {
        if (json instanceof JsonObject) {
            json = ((JsonObject) json).getMap();
        } else if (json instanceof JsonArray) {
            json = ((JsonArray) json).getList();
        }
        if (json instanceof Map) {
            generator.writeStartObject();
            for (Map.Entry<String, ?> e : ((Map<String, ?>) json).entrySet()) {
                generator.writeFieldName(e.getKey());
                encodeJson(e.getValue(), generator);
            }
            generator.writeEndObject();
        } else if (json instanceof List) {
            generator.writeStartArray();
            for (Object item : (List<?>) json) {
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

    public static <T> T decode(String json, Class<T> clazz) throws DecodeException {
        JsonParser parser = null;
        Object res;
        JsonToken remaining;
        try {
            parser = factory.createParser(json);
            parser.nextToken();
            res = parseAny(parser);
            remaining = parser.nextToken();
        } catch (IOException e) {
            throw new DecodeException("Failed to decode:" + e.getMessage(), e);
        } finally {
            close(parser);
        }
        if (remaining != null) {
            throw new DecodeException("Unexpected trailing token");
        }
        return cast(res, clazz);
    }

    private static Object parseAny(JsonParser parser) throws IOException, DecodeException {
        switch (parser.currentTokenId()) {
        case JsonTokenId.ID_START_OBJECT:
            return parseObject(parser);
        case JsonTokenId.ID_START_ARRAY:
            return parseArray(parser);
        case JsonTokenId.ID_STRING:
            return parser.getText();
        case JsonTokenId.ID_NUMBER_FLOAT:
        case JsonTokenId.ID_NUMBER_INT:
            return parser.getNumberValue();
        case JsonTokenId.ID_TRUE:
            return Boolean.TRUE;
        case JsonTokenId.ID_FALSE:
            return Boolean.FALSE;
        case JsonTokenId.ID_NULL:
            return null;
        default:
            throw new DecodeException("Unexpected token"/*, parser.getCurrentLocation()*/);
        }
    }

    private static Map<String, Object> parseObject(JsonParser parser) throws IOException {
        String key1 = parser.nextFieldName();
        if (key1 == null) {
            return new LinkedHashMap<>(2);
        }
        parser.nextToken();
        Object value1 = parseAny(parser);
        String key2 = parser.nextFieldName();
        if (key2 == null) {
            LinkedHashMap<String, Object> obj = new LinkedHashMap<>(2);
            obj.put(key1, value1);
            return obj;
        }
        parser.nextToken();
        Object value2 = parseAny(parser);
        String key = parser.nextFieldName();
        if (key == null) {
            LinkedHashMap<String, Object> obj = new LinkedHashMap<>(2);
            obj.put(key1, value1);
            obj.put(key2, value2);
            return obj;
        }
        // General case
        LinkedHashMap<String, Object> obj = new LinkedHashMap<>();
        obj.put(key1, value1);
        obj.put(key2, value2);
        do {
            parser.nextToken();
            Object value = parseAny(parser);
            obj.put(key, value);
            key = parser.nextFieldName();
        } while (key != null);
        return obj;
    }

    private static List<Object> parseArray(JsonParser parser) throws IOException {
        List<Object> array = new ArrayList<>();
        while (true) {
            parser.nextToken();
            int tokenId = parser.currentTokenId();
            if (tokenId == JsonTokenId.ID_FIELD_NAME) {
                throw new UnsupportedOperationException();
            } else if (tokenId == JsonTokenId.ID_END_ARRAY) {
                return array;
            }
            Object value = parseAny(parser);
            array.add(value);
        }
    }

    private static <T> T cast(Object o, Class<T> clazz) {
        if (o instanceof Map) {
            if (!clazz.isAssignableFrom(Map.class)) {
                throw new DecodeException("Failed to decode");
            }
            if (clazz == Object.class) {
                o = new JsonObject((Map) o);
            }
            return clazz.cast(o);
        } else if (o instanceof List) {
            if (!clazz.isAssignableFrom(List.class)) {
                throw new DecodeException("Failed to decode");
            }
            if (clazz == Object.class) {
                o = new JsonArray((List) o);
            }
            return clazz.cast(o);
        } else if (o instanceof String) {
            if (!clazz.isAssignableFrom(String.class)) {
                throw new DecodeException("Failed to decode");
            }
            return clazz.cast(o);
        } else if (o instanceof Boolean) {
            if (!clazz.isAssignableFrom(Boolean.class)) {
                throw new DecodeException("Failed to decode");
            }
            return clazz.cast(o);
        } else if (o == null) {
            return null;
        } else {
            Number number = (Number) o;
            if (clazz == Integer.class) {
                o = number.intValue();
            } else if (clazz == Long.class) {
                o = number.longValue();
            } else if (clazz == Float.class) {
                o = number.floatValue();
            } else if (clazz == Double.class) {
                o = number.doubleValue();
            } else if (clazz == Byte.class) {
                o = number.byteValue();
            } else if (clazz == Short.class) {
                o = number.shortValue();
            } else if (clazz == Object.class || clazz.isAssignableFrom(Number.class)) {
                // Nothing
            } else {
                throw new DecodeException("Failed to decode");
            }
            return clazz.cast(o);
        }
    }

    private static void close(Closeable closeable) {
        try {
            if (closeable != null)
                closeable.close();
        } catch (IOException ignore) {
        }
    }
}

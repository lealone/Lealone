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

import java.time.Instant;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author zhh
 */
@SuppressWarnings({ "rawtypes" })
public abstract class Json {

    public static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();
    public static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

    /**
     * Encode a POJO to JSON using the underlying Jackson mapper.
     *
     * @param obj a POJO
     * @return a String containing the JSON representation of the given POJO.
     * @throws EncodeException if a property cannot be encoded.
     */
    public static String encode(Object obj) throws EncodeException {
        return JacksonCodec.encode(obj, false);
    }

    /**
     * Encode a POJO to JSON with pretty indentation, using the underlying Jackson mapper.
     *
     * @param obj a POJO
     * @return a String containing the JSON representation of the given POJO.
     * @throws EncodeException if a property cannot be encoded.
     */
    public static String encodePrettily(Object obj) throws EncodeException {
        return JacksonCodec.encode(obj, true);
    }

    /**
     * Decode a given JSON string to a POJO of the given class type.
     * @param str the JSON string.
     * @param clazz the class to map to.
     * @param <T> the generic type.
     * @return an instance of T
     * @throws DecodeException when there is a parsing or invalid mapping.
     */
    public static <T> T decode(String str, Class<T> clazz) throws DecodeException {
        return JacksonCodec.decode(str, clazz);
    }

    /**
     * Decode a given JSON string.
     *
     * @param str the JSON string.
     *
     * @return a JSON element which can be a {@link JsonArray}, {@link JsonObject}, {@link String}, 
     *         ...etc if the content is an array, object, string, ...etc
     * @throws DecodeException when there is a parsing or invalid mapping.
     */
    public static Object decode(String str) throws DecodeException {
        return JacksonCodec.decode(str, Object.class);
    }

    protected String getString(Object val) {
        if (val == null) {
            return null;
        }
        if (val instanceof Instant) {
            return ISO_INSTANT.format((Instant) val);
        } else if (val instanceof byte[]) {
            return BASE64_ENCODER.encodeToString((byte[]) val);
        } else if (val instanceof Enum) {
            return ((Enum) val).name();
        } else {
            return val.toString();
        }
    }

    protected Integer getInteger(Object val) {
        Number number = (Number) val;
        if (number == null) {
            return null;
        } else if (number instanceof Integer) {
            return (Integer) number; // Avoids unnecessary unbox/box
        } else {
            return number.intValue();
        }
    }

    protected Long getLong(Object val) {
        Number number = (Number) val;
        if (number == null) {
            return null;
        } else if (number instanceof Long) {
            return (Long) number; // Avoids unnecessary unbox/box
        } else {
            return number.longValue();
        }
    }

    protected Double getDouble(Object val) {
        Number number = (Number) val;
        if (number == null) {
            return null;
        } else if (number instanceof Double) {
            return (Double) number; // Avoids unnecessary unbox/box
        } else {
            return number.doubleValue();
        }
    }

    protected Float getFloat(Object val) {
        Number number = (Number) val;
        if (number == null) {
            return null;
        } else if (number instanceof Float) {
            return (Float) number; // Avoids unnecessary unbox/box
        } else {
            return number.floatValue();
        }
    }

    protected byte[] getBinary(Object val) {
        // no-op
        if (val == null) {
            return null;
        }
        // no-op if value is already an byte[]
        if (val instanceof byte[]) {
            return (byte[]) val;
        }
        // assume that the value is in String format as per RFC
        String encoded = (String) val;
        // parse to proper type
        return BASE64_DECODER.decode(encoded);
    }

    protected Instant getInstant(Object val) {
        // no-op
        if (val == null) {
            return null;
        }
        // no-op if value is already an Instant
        if (val instanceof Instant) {
            return (Instant) val;
        }
        // assume that the value is in String format as per RFC
        String encoded = (String) val;
        // parse to proper type
        return Instant.from(ISO_INSTANT.parse(encoded));
    }

    protected boolean valueEquals(Object thisValue, Object otherValue) {
        // identity check
        if (thisValue == otherValue) {
            return true;
        }
        // special case for numbers
        if (thisValue instanceof Number && otherValue instanceof Number
                && thisValue.getClass() != otherValue.getClass()) {
            Number n1 = (Number) thisValue;
            Number n2 = (Number) otherValue;
            // floating point values
            if (thisValue instanceof Float || thisValue instanceof Double || otherValue instanceof Float
                    || otherValue instanceof Double) {
                // compare as floating point double
                if (n1.doubleValue() == n2.doubleValue()) {
                    // same value check the next entry
                    return true;
                }
            }
            if (thisValue instanceof Integer || thisValue instanceof Long || otherValue instanceof Integer
                    || otherValue instanceof Long) {
                // compare as integer long
                if (n1.longValue() == n2.longValue()) {
                    // same value check the next entry
                    return true;
                }
            }
        }
        // special case for char sequences
        if (thisValue instanceof CharSequence && otherValue instanceof CharSequence
                && thisValue.getClass() != otherValue.getClass()) {
            CharSequence s1 = (CharSequence) thisValue;
            CharSequence s2 = (CharSequence) otherValue;

            if (Objects.equals(s1.toString(), s2.toString())) {
                // same value check the next entry
                return true;
            }
        }
        // fallback to standard object equals checks
        if (!Objects.equals(thisValue, otherValue)) {
            return false;
        }
        return true;
    }

    /**
     * Wraps well known java types to adhere to the Json expected types.
     * <ul>
     *   <li>{@code Map} will be wrapped to {@code JsonObject}</li>
     *   <li>{@code List} will be wrapped to {@code JsonArray}</li>
     *   <li>{@code Instant} will be converted to iso date {@code String}</li>
     *   <li>{@code byte[]} will be converted to base64 {@code String}</li>
     *   <li>{@code Enum} will be converted to enum name {@code String}</li>
     * </ul>
     *
     * @param val java type
     * @return wrapped type or {@code val} if not applicable.
     */
    @SuppressWarnings("unchecked")
    protected static Object wrapJsonValue(Object val) {
        if (val == null) {
            return null;
        }
        // perform wrapping
        if (val instanceof Map) {
            val = new JsonObject((Map) val);
        } else if (val instanceof List) {
            val = new JsonArray((List) val);
        } else if (val instanceof Instant) {
            val = ISO_INSTANT.format((Instant) val);
        } else if (val instanceof byte[]) {
            val = BASE64_ENCODER.encodeToString((byte[]) val);
        } else if (val instanceof Enum) {
            val = ((Enum) val).name();
        }
        return val;
    }

    static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
        Iterable<T> iterable = () -> sourceIterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}

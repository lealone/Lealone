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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.service.JsonArrayGetter;
import org.lealone.db.value.ReadonlyArray;
import org.lealone.db.value.Value;

/**
 * A representation of a <a href="http://json.org/">JSON</a> array in Java.
 *
 * Unlike some other languages Java does not have a native understanding of JSON. To enable JSON to be used easily
 * in Vert.x code we use this class to encapsulate the notion of a JSON array.
 *
 * The implementation adheres to the <a href="http://rfc-editor.org/rfc/rfc7493.txt">RFC-7493</a> to support Temporal
 * data types as well as binary data.
 *
 * Please see the documentation for more information.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author zhh
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class JsonArray extends Json implements Iterable<Object> {

    private List<Object> list;

    /**
     * Create an empty instance
     */
    public JsonArray() {
        list = new ArrayList<>();
    }

    /**
     * Create an instance from a List. The List is not copied.
     *
     * @param list the underlying backing list
     */
    public JsonArray(List list) {
        if (list == null) {
            throw new NullPointerException();
        }
        this.list = list;
    }

    /**
     * Create an instance from a String of JSON, this string must be a valid array otherwise an exception will be thrown.
     * <p/>
     * If you are unsure of the value, you should use instead {@link Json#decode(String)} and check the result is
     * a JSON array.
     *
     * @param json the string of JSON
     */
    public JsonArray(String json) {
        if (json == null) {
            throw new NullPointerException();
        }
        list = JacksonCodec.decode(json, List.class);
        if (list == null) {
            throw new DecodeException("Invalid JSON array: " + json);
        }
    }

    /**
     * Get the String at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the String (or String representation), or null if a null value present
     */
    public String getString(int pos) {
        return getString(list.get(pos));
    }

    /**
     * Get the Integer at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the Integer, or null if a null value present
     * @throws java.lang.ClassCastException if the value cannot be converted to Integer
     */
    public Integer getInteger(int pos) {
        return getInteger(list.get(pos));
    }

    /**
     * Get the Long at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the Long, or null if a null value present
     * @throws java.lang.ClassCastException if the value cannot be converted to Long
     */
    public Long getLong(int pos) {
        return getLong(list.get(pos));
    }

    /**
     * Get the Double at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the Double, or null if a null value present
     * @throws java.lang.ClassCastException if the value cannot be converted to Double
     */
    public Double getDouble(int pos) {
        return getDouble(list.get(pos));
    }

    /**
     * Get the Float at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the Float, or null if a null value present
     * @throws java.lang.ClassCastException if the value cannot be converted to Float
     */
    public Float getFloat(int pos) {
        return getFloat(list.get(pos));
    }

    /**
     * Get the Boolean at position {@code pos} in the array,
     *
     * @param pos the position in the array
     * @return the Boolean, or null if a null value present
     * @throws java.lang.ClassCastException if the value cannot be converted to Integer
     */
    public Boolean getBoolean(int pos) {
        return (Boolean) list.get(pos);
    }

    /**
     * Get the JsonObject at position {@code pos} in the array.
     *
     * @param pos the position in the array
     * @return the JsonObject, or null if a null value present
     * @throws java.lang.ClassCastException if the value cannot be converted to JsonObject
     */
    public JsonObject getJsonObject(int pos) {
        Object val = list.get(pos);
        if (val instanceof Map) {
            val = new JsonObject((Map) val);
        }
        return (JsonObject) val;
    }

    /**
     * Get the JsonArray at position {@code pos} in the array.
     *
     * @param pos the position in the array
     * @return the Integer, or null if a null value present
     * @throws java.lang.ClassCastException if the value cannot be converted to JsonArray
     */
    public JsonArray getJsonArray(int pos) {
        Object val = list.get(pos);
        if (val instanceof List) {
            val = new JsonArray((List) val);
        }
        return (JsonArray) val;
    }

    /**
     * Get the byte[] at position {@code pos} in the array.
     *
     * JSON itself has no notion of a binary, so this method assumes there is a String value and
     * it contains a Base64 encoded binary, which it decodes if found and returns.
     *
     * @param pos the position in the array
     * @return the byte[], or null if a null value present
     * @throws java.lang.ClassCastException       if the value cannot be converted to String
     * @throws java.lang.IllegalArgumentException if the String value is not a legal Base64 encoded value
     */
    public byte[] getBinary(int pos) {
        return getBinary(list.get(pos));
    }

    /**
     * Get the Instant at position {@code pos} in the array.
     *
     * JSON itself has no notion of a temporal types, this extension complies to the RFC-7493, so this method assumes
     * there is a String value and it contains an ISO 8601 encoded date and time format such as "2017-04-03T10:25:41Z",
     * which it decodes if found and returns.
     *
     * @param pos the position in the array
     * @return the Instant, or null if a null value present
     * @throws java.lang.ClassCastException            if the value cannot be converted to String
     * @throws java.time.format.DateTimeParseException if the String value is not a legal ISO 8601 encoded value
     */
    public Instant getInstant(int pos) {
        return getInstant(list.get(pos));
    }

    /**
     * Get the value with the specified key, as an Object with types respecting the limitations of JSON.
     * <ul>
     *   <li>{@code Map} will be wrapped to {@code JsonObject}</li>
     *   <li>{@code List} will be wrapped to {@code JsonArray}</li>
     *   <li>{@code Instant} will be converted to {@code String}</li>
     *   <li>{@code byte[]} will be converted to {@code String}</li>
     *   <li>{@code Enum} will be converted to {@code String}</li>
     * </ul>
     *
     * @param pos the position in the array
     * @return the Integer, or null if a null value present
     */
    public Object getValue(int pos) {
        return wrapJsonValue(list.get(pos));
    }

    /**
     * Is there a null value at position pos?
     *
     * @param pos the position in the array
     * @return true if null value present, false otherwise
     */
    public boolean hasNull(int pos) {
        return list.get(pos) == null;
    }

    /**
     * Add a null value to the JSON array.
     *
     * @return a reference to this, so the API can be used fluently
     */
    public JsonArray addNull() {
        list.add(null);
        return this;
    }

    /**
     * Add an Object to the JSON array.
     *
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public JsonArray add(Object value) {
        list.add(value);
        return this;
    }

    /**
     * Add an Object to the JSON array at given position {@code pos}.
     *
     * @param pos the position
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public JsonArray add(int pos, Object value) {
        list.add(pos, value);
        return this;
    }

    /**
     * Appends all of the elements in the specified array to the end of this JSON array.
     *
     * @param array the array
     * @return a reference to this, so the API can be used fluently
     */
    public JsonArray addAll(JsonArray array) {
        list.addAll(array.list);
        return this;
    }

    /**
     * Set a null value to the JSON array at position {@code pos}.
     *
     * @return a reference to this, so the API can be used fluently
     */
    public JsonArray setNull(int pos) {
        list.set(pos, null);
        return this;
    }

    /**
     * Set an Object to the JSON array at position {@code pos}.
     *
     * @param pos   position in the array
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public JsonArray set(int pos, Object value) {
        list.set(pos, value);
        return this;
    }

    /**
     * Does the JSON array contain the specified value? This method will scan the entire array until it finds a value
     * or reaches the end.
     *
     * @param value the value
     * @return true if it contains the value, false if not
     */
    public boolean contains(Object value) {
        return list.contains(value);
    }

    /**
     * Remove the specified value from the JSON array. This method will scan the entire array until it finds a value
     * or reaches the end.
     *
     * @param value the value to remove
     * @return true if it removed it, false if not found
     */
    public boolean remove(Object value) {
        final Object wrappedValue = wrapJsonValue(value);
        for (int i = 0; i < list.size(); i++) {
            // perform comparision on wrapped types
            final Object otherWrapperValue = getValue(i);
            if (wrappedValue == null) {
                if (otherWrapperValue == null) {
                    list.remove(i);
                    return true;
                }
            } else {
                if (wrappedValue.equals(otherWrapperValue)) {
                    list.remove(i);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Remove the value at the specified position in the JSON array.
     *
     * @param pos the position to remove the value at
     * @return the removed value if removed, null otherwise. If the value is a Map, a {@link JsonObject} is built from
     * this Map and returned. It the value is a List, a {@link JsonArray} is built form this List and returned.
     */
    public Object remove(int pos) {
        return wrapJsonValue(list.remove(pos));
    }

    /**
     * Get the number of values in this JSON array
     *
     * @return the number of items
     */
    public int size() {
        return list.size();
    }

    /**
     * Are there zero items in this JSON array?
     *
     * @return true if zero, false otherwise
     */
    public boolean isEmpty() {
        return list.isEmpty();
    }

    /**
     * Get the unerlying List
     *
     * @return the underlying List
     */
    public List getList() {
        return list;
    }

    /**
     * Remove all entries from the JSON array
     *
     * @return a reference to this, so the API can be used fluently
     */
    public JsonArray clear() {
        list.clear();
        return this;
    }

    /**
     * Encode the JSON array to a string
     *
     * @return the string encoding
     */
    public String encode() {
        return JacksonCodec.encode(this, false);
    }

    /**
     * Encode the JSON array prettily as a string
     *
     * @return the string encoding
     */
    public String encodePrettily() {
        return JacksonCodec.encode(this, true);
    }

    /**
     * Get an Iterator over the values in the JSON array
     *
     * @return an iterator
     */
    @Override
    public Iterator<Object> iterator() {
        return new Iter(list.iterator());
    }

    /**
     * Get a Stream over the entries in the JSON array
     *
     * @return a Stream
     */
    public Stream<Object> stream() {
        return asStream(iterator());
    }

    @Override
    public String toString() {
        return encode();
    }

    @Override
    public int hashCode() {
        return list.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        // null check
        if (o == null)
            return false;
        // self check
        if (this == o)
            return true;
        // type check and cast
        if (getClass() != o.getClass())
            return false;

        JsonArray other = (JsonArray) o;
        // size check
        if (this.size() != other.size())
            return false;
        // value comparison
        for (int i = 0; i < this.size(); i++) {
            Object thisValue = this.getValue(i);
            Object otherValue = other.getValue(i);
            if (valueEquals(thisValue, otherValue)) {
                continue;
            }
        }
        // all checks passed
        return true;
    }

    private static class Iter implements Iterator<Object> {

        final Iterator<Object> listIter;

        Iter(Iterator<Object> listIter) {
            this.listIter = listIter;
        }

        @Override
        public boolean hasNext() {
            return listIter.hasNext();
        }

        @Override
        public Object next() {
            return wrapJsonValue(listIter.next());
        }

        @Override
        public void remove() {
            listIter.remove();
        }
    }

    public static class Getter implements JsonArrayGetter {

        private JsonArray ja;

        @Override
        public void init(String json) {
            ja = new JsonArray(json);
        }

        @Override
        public Object getValue(int i, int type) {
            Object v;
            switch (type) {
            case Value.BOOLEAN:
                v = Boolean.valueOf(ja.getValue(i).toString());
                break;
            case Value.BYTE:
                v = Byte.valueOf(ja.getValue(i).toString());
                break;
            case Value.SHORT:
                v = Short.valueOf(ja.getValue(i).toString());
                break;
            case Value.INT:
                v = Integer.valueOf(ja.getValue(i).toString());
                break;
            case Value.LONG:
                v = Long.valueOf(ja.getValue(i).toString());
                break;
            case Value.DECIMAL:
                v = new java.math.BigDecimal(ja.getValue(i).toString());
                break;
            case Value.TIME:
                v = java.sql.Time.valueOf(ja.getValue(i).toString());
                break;
            case Value.DATE:
                v = java.sql.Date.valueOf(ja.getValue(i).toString());
                break;
            case Value.TIMESTAMP:
                v = java.sql.Timestamp.valueOf(ja.getValue(i).toString());
                break;
            case Value.BYTES:
                v = ja.getString(i).getBytes();
                break;
            case Value.UUID:
                v = java.util.UUID.fromString(ja.getValue(i).toString());
                break;
            case Value.STRING:
            case Value.STRING_IGNORECASE:
            case Value.STRING_FIXED:
                v = ja.getString(i);
                break;
            case Value.BLOB:
                v = new org.lealone.db.value.ReadonlyBlob(ja.getString(i));
                break;
            case Value.CLOB:
                v = new org.lealone.db.value.ReadonlyClob(ja.getString(i));
                break;
            case Value.DOUBLE:
                v = Double.valueOf(ja.getValue(i).toString());
                break;
            case Value.FLOAT:
                v = Float.valueOf(ja.getValue(i).toString());
                break;
            case Value.NULL:
                return null;
            case Value.JAVA_OBJECT:
                v = ja.getValue(i);
                break;
            case Value.UNKNOWN:
                v = ja.getValue(i);
                break;
            case Value.ARRAY:
                v = new ReadonlyArray(ja.getJsonArray(i).getList());
                break;
            case Value.RESULT_SET:
                v = ja.getValue(i);
                break;
            default:
                throw DbException.getInternalError("type=" + type);
            }
            return v;
        }
    }
}

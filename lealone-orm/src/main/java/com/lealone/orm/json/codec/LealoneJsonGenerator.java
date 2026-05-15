/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.json.codec;

public class LealoneJsonGenerator {

    private final StringBuilder json = new StringBuilder();

    public void writeStartObject() {
        json.append('{');
    }

    public void writeEndObject() {
        json.append('}');
    }

    public void writeStartArray() {
        json.append('[');
    }

    public void writeEndArray() {
        json.append(']');
    }

    public void writeFieldName(String name) {
        json.append('"').append(name).append('"').append(':');
    }

    public void writeNumber(Number number) {
        json.append(number);
    }

    public void writeBoolean(boolean b) {
        json.append(b);
    }

    public void writeString(String text) {
        json.append('"');
        jsonEscape(text);
        json.append('"');
    }

    public void writeNull() {
        json.append("null");
    }

    public void writeSeparatorChar() {
        json.append(',');
    }

    @Override
    public String toString() {
        return json.toString();
    }

    public void jsonEscape(String s) {
        if (s == null)
            return;
        for (char c : s.toCharArray()) {
            switch (c) {
            case '"':
                json.append("\\\"");
                break;
            case '\\':
                json.append("\\\\");
                break;
            case '\n':
                json.append("\\n");
                break;
            case '\r':
                json.append("\\r");
                break;
            case '\t':
                json.append("\\t");
                break;
            case '\b':
                json.append("\\b");
                break;
            case '\f':
                json.append("\\f");
                break;
            default:
                json.append(c);
            }
        }
    }
}

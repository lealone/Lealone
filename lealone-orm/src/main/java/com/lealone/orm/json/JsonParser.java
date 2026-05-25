/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.lealone.sql.SQLParserBase;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ExpressionColumn;

public class JsonParser extends SQLParserBase {

    public JsonParser() {
        isJson = true;
    }

    public Map<String, Object> parseJsonObject(String json) {
        initialize(json);
        read();
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        if (readIfChar('{')) {
            parseJsonObject(map);
        }
        return map;
    }

    public ArrayList<Object> parseJsonArray(String json) {
        initialize(json);
        read();
        if (readIfChar('[')) {
            return parseJsonArray();
        }
        return new ArrayList<>();
    }

    public Object parseJsonAny(String json) {
        initialize(json);
        read();
        if (readIfChar('{')) {
            LinkedHashMap<String, Object> map = new LinkedHashMap<>();
            parseJsonObject(map);
            return map;
        } else if (readIfChar('[')) {
            return parseJsonArray();
        } else {
            return readValue();
        }
    }

    private void parseJsonObject(Map<String, Object> map) {
        if (readIfChar('}'))
            return;
        do {
            String key = null;
            if (currentTokenType == IDENTIFIER) {
                key = currentToken;
                read();
                read(":");
            }
            if (currentTokenType == IDENTIFIER) {
                map.put(key, currentToken);
                read();
            } else if (currentTokenType == VALUE) {
                map.put(key, currentValue.getObject());
                read();
            } else if (readIfChar('[')) {
                map.put(key, parseJsonArray());
            } else if (readIfChar('{')) {
                LinkedHashMap<String, Object> map2 = new LinkedHashMap<>();
                parseJsonObject(map2);
                map.put(key, map2);
            } else {
                map.put(key, readValue());
            }
        } while (readIfChar(','));
        read("}");
    }

    private Object readValue() {
        if (currentTokenType == IDENTIFIER) {
            String v = currentToken;
            read();
            return v;
        } else if (currentTokenType == VALUE) {
            Object v = currentValue.getObject();
            read();
            return v;
        } else {
            Expression v = readTerm();
            if (v instanceof ExpressionColumn c)
                return c.getColumnName();
            else
                return v.getValue(session).getObject();
        }
    }

    private boolean readIfChar(char c) {
        if (!currentTokenQuoted && currentToken.length() > 0 && currentToken.charAt(0) == c) {
            read();
            return true;
        }
        if (expectedList != null) {
            expectedList.add(String.valueOf(c));
        }
        return false;
    }

    private ArrayList<Object> parseJsonArray() {
        ArrayList<Object> list = new ArrayList<>();
        if (readIfChar(']'))
            return list;
        do {
            if (currentTokenType == IDENTIFIER) {
                list.add(currentToken);
                read();
            } else if (currentTokenType == VALUE) {
                list.add(currentValue.getObject());
                read();
            } else if (readIfChar('[')) {
                list.add(parseJsonArray());
            } else if (readIfChar('{')) {
                LinkedHashMap<String, Object> map = new LinkedHashMap<>();
                parseJsonObject(map);
                list.add(map);
            } else {
                list.add(readValue());
            }
        } while (readIfChar(','));
        read("]");
        return list;
    }
}

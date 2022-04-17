/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.service;

import org.lealone.common.exceptions.DbException;

public interface JsonArrayGetter {

    public static JsonArrayGetter create(String json) {
        try {
            String className = "org.lealone.orm.json.JsonArray.Getter";
            JsonArrayGetter getter = (JsonArrayGetter) Class.forName(className).getDeclaredConstructor().newInstance();
            getter.init(json);
            return getter;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    void init(String json);

    Object getValue(int pos, int type);
}

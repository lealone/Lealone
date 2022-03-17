/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.service;

import org.lealone.common.exceptions.DbException;

public interface JsonArrayDecoder {

    public static JsonArrayDecoder create(String json) {
        try {
            JsonArrayDecoder decoder = (JsonArrayDecoder) Class.forName("org.lealone.orm.json.JsonArrayDecoderImpl")
                    .getDeclaredConstructor().newInstance();
            decoder.init(json);
            return decoder;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    void init(String json);

    Object getValue(int pos, int type);
}

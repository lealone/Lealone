/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.json.codec;

import java.util.List;
import java.util.Map;

public interface JsonCodec {

    public String encode(Object object, boolean pretty);

    public Map<String, Object> decodeJsonObject(String json);

    public List<Object> decodeJsonArray(String json);

    public Object decodeAny(String json);

}

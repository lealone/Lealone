/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.service;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;

public interface ServiceExecutor {

    final String NO_RETURN_VALUE = "__NO_RETURN_VALUE__";

    default Value executeService(String methodName, Value[] methodArgs) {
        return ValueNull.INSTANCE;
    }

    default String executeService(String methodName, Map<String, Object> methodArgs) {
        return NO_RETURN_VALUE;
    }

    default String executeService(String methodName, String json) {
        return NO_RETURN_VALUE;
    }

    public static String toString(String key, Map<String, Object> methodArgs) {
        Object v = methodArgs.get(key);
        if (v == null)
            return null;
        else {
            return v.toString().trim();
        }
    }

    public static byte[] toBytes(String key, Map<String, Object> methodArgs) {
        Object v = methodArgs.get(key);
        if (v == null)
            return null;
        else {
            return v.toString().trim().getBytes();
        }
    }
}

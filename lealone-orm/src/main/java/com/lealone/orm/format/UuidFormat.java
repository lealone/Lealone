/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import java.util.UUID;

public class UuidFormat implements TypeFormat<UUID> {

    @Override
    public Object encode(UUID v) {
        return v.toString();
    }

    @Override
    public UUID decode(Object v) {
        return UUID.fromString(v.toString());
    }
}

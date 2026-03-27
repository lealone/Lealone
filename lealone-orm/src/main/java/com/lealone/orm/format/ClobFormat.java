/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import java.sql.Clob;

import com.lealone.db.value.ReadonlyClob;
import com.lealone.orm.property.PClob;

public class ClobFormat implements TypeFormat<Clob> {

    @Override
    public Object encode(Clob v) {
        return PClob.toString(v);
    }

    @Override
    public Clob decode(Object v) {
        return new ReadonlyClob(v.toString());
    }
}

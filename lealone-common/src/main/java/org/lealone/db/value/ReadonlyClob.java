/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.value;

import java.io.Writer;
import java.sql.SQLException;

import org.lealone.common.trace.Trace;

/**
 * Represents a readonly CLOB value.
 */
public class ReadonlyClob extends ClobBase {

    public ReadonlyClob(Value value) {
        this.value = value;
        this.trace = Trace.NO_TRACE;
    }

    public ReadonlyClob(String value) {
        this(ValueString.get(value));
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        throw unsupported("LOB update");
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        throw unsupported("LOB update");
    }
}

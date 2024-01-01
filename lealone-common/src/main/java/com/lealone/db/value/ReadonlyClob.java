/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.value;

import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.SQLException;

import com.lealone.common.trace.Trace;
import com.lealone.db.LocalDataHandler;

/**
 * Represents a readonly CLOB value.
 */
public class ReadonlyClob extends ClobBase {

    public ReadonlyClob(Value value) {
        this.value = value;
        this.trace = Trace.NO_TRACE;
    }

    public ReadonlyClob(String value) {
        this(new StringReader(value));
    }

    public ReadonlyClob(Reader reader) {
        this(new LocalDataHandler().getLobStorage().createClob(reader, -1));
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

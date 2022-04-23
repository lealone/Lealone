/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.value;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

import org.lealone.common.trace.Trace;
import org.lealone.db.LocalDataHandler;

/**
 * Represents a readonly BLOB value.
 */
public class ReadonlyBlob extends BlobBase {

    public ReadonlyBlob(Value value) {
        this.value = value;
        this.trace = Trace.NO_TRACE;
    }

    public ReadonlyBlob(String value) {
        this(ValueString.get(value));
    }

    public ReadonlyBlob(byte[] bytes) {
        this(new ByteArrayInputStream(bytes));
    }

    public ReadonlyBlob(InputStream in) {
        this(new LocalDataHandler().getLobStorage().createBlob(in, -1));
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw unsupported("LOB update");
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw unsupported("LOB update");
    }
}

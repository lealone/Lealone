/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.jdbc;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.TraceObjectType;
import org.lealone.common.util.Task;
import org.lealone.db.value.BlobBase;
import org.lealone.db.value.Value;

/**
 * Represents a BLOB value.
 * 
 * @author H2 Group
 * @author zhh
 */
public class JdbcBlob extends BlobBase {

    private final JdbcConnection conn;

    /**
     * INTERNAL
     */
    public JdbcBlob(JdbcConnection conn, Value value, int id) {
        this.conn = conn;
        this.value = value;
        this.trace = conn.getTrace(TraceObjectType.BLOB, id);
    }

    @Override
    protected void checkClosed() {
        conn.checkClosed();
        super.checkClosed();
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBytes(" + pos + ", " + quoteBytes(bytes) + ");");
            }
            checkClosed();
            if (pos != 1) {
                throw DbException.getInvalidValueException("pos", pos);
            }
            value = conn.createBlob(new ByteArrayInputStream(bytes), -1);
            return bytes.length;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBinaryStream(" + pos + ");");
            }
            checkClosed();
            if (pos != 1) {
                throw DbException.getInvalidValueException("pos", pos);
            }
            if (value.getPrecision() != 0) {
                throw DbException.getInvalidValueException("length", value.getPrecision());
            }
            final JdbcConnection c = conn;
            final PipedInputStream in = new PipedInputStream();
            final Task task = new Task() {
                @Override
                public void call() {
                    value = c.createBlob(in, -1);
                }
            };
            PipedOutputStream out = new PipedOutputStream(in) {
                @Override
                public void close() throws IOException {
                    super.close();
                    try {
                        task.get();
                    } catch (Exception e) {
                        throw DbException.convertToIOException(e);
                    }
                }
            };
            task.execute();
            return new BufferedOutputStream(out);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
}

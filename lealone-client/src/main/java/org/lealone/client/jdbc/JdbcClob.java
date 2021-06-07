/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.jdbc;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.StringReader;
import java.io.Writer;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.TraceObjectType;
import org.lealone.common.util.IOUtils;
import org.lealone.common.util.Task;
import org.lealone.db.value.ClobBase;
import org.lealone.db.value.Value;

/**
 * Represents a CLOB value.
 */
public class JdbcClob extends ClobBase {

    private final JdbcConnection conn;

    /**
     * INTERNAL
     */
    public JdbcClob(JdbcConnection conn, Value value, int id) {
        this.conn = conn;
        this.value = value;
        this.trace = conn.getTrace(TraceObjectType.CLOB, id);
    }

    @Override
    protected void checkClosed() {
        conn.checkClosed();
        super.checkClosed();
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCodeCall("setCharacterStream(" + pos + ");");
            }
            checkClosed();
            if (pos != 1) {
                throw DbException.getInvalidValueException("pos", pos);
            }
            if (value.getPrecision() != 0) {
                throw DbException.getInvalidValueException("length", value.getPrecision());
            }
            final JdbcConnection c = conn;
            // PipedReader / PipedWriter are a lot slower
            // than PipedInputStream / PipedOutputStream
            // (Sun/Oracle Java 1.6.0_20)
            final PipedInputStream in = new PipedInputStream();
            final Task task = new Task() {
                @Override
                public void call() {
                    value = c.createClob(IOUtils.getReader(in), -1);
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
            return IOUtils.getBufferedWriter(out);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setString(" + pos + ", " + quote(str) + ");");
            }
            checkClosed();
            if (pos != 1) {
                throw DbException.getInvalidValueException("pos", pos);
            } else if (str == null) {
                throw DbException.getInvalidValueException("str", str);
            }
            value = conn.createClob(new StringReader(str), -1);
            return str.length();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
}

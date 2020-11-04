/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.value;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceObject;
import org.lealone.common.util.IOUtils;
import org.lealone.db.Constants;
import org.lealone.db.api.ErrorCode;

/**
 * Represents a CLOB value.
 */
public class ReadonlyClob extends TraceObject implements Clob, NClob {

    private Value value;

    public ReadonlyClob(Value value) {
        this.value = value;
        this.trace = Trace.NO_TRACE;
    }

    /**
     * Returns the length.
     *
     * @return the length
     */
    @Override
    public long length() throws SQLException {
        try {
            debugCodeCall("length");
            checkClosed();
            if (value.getType() == Value.CLOB) {
                long precision = value.getPrecision();
                if (precision > 0) {
                    return precision;
                }
            }
            return IOUtils.copyAndCloseInput(value.getReader(), null, Long.MAX_VALUE);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Truncates the object.
     */
    @Override
    public void truncate(long len) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * Returns the input stream.
     *
     * @return the input stream
     */
    @Override
    public InputStream getAsciiStream() throws SQLException {
        try {
            debugCodeCall("getAsciiStream");
            checkClosed();
            String s = value.getString();
            return IOUtils.getInputStreamFromString(s);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Returns an output  stream.
     */
    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * Returns the reader.
     *
     * @return the reader
     */
    @Override
    public Reader getCharacterStream() throws SQLException {
        try {
            debugCodeCall("getCharacterStream");
            checkClosed();
            return value.getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get a writer to update the Clob. This is only supported for new, empty
     * Clob objects that were created with Connection.createClob() or
     * createNClob(). The Clob is created in a separate thread, and the object
     * is only updated when Writer.close() is called. The position must be 1,
     * meaning the whole Clob data is set.
     *
     * @param pos where to start writing (the first character is at position 1)
     * @return a writer
     */
    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * Returns a substring.
     *
     * @param pos the position (the first character is at position 1)
     * @param length the number of characters
     * @return the string
     */
    @Override
    public String getSubString(long pos, int length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getSubString(" + pos + ", " + length + ");");
            }
            checkClosed();
            if (pos < 1) {
                throw DbException.getInvalidValueException("pos", pos);
            }
            if (length < 0) {
                throw DbException.getInvalidValueException("length", length);
            }
            StringWriter writer = new StringWriter(Math.min(Constants.IO_BUFFER_SIZE, length));
            Reader reader = value.getReader();
            try {
                IOUtils.skipFully(reader, pos - 1);
                IOUtils.copyAndCloseInput(reader, writer, length);
            } finally {
                reader.close();
            }
            return writer.toString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Fills the Clob. This is only supported for new, empty Clob objects that
     * were created with Connection.createClob() or createNClob(). The position
     * must be 1, meaning the whole Clob data is set.
     *
     * @param pos where to start writing (the first character is at position 1)
     * @param str the string to add
     * @return the length of the added text
     */
    @Override
    public int setString(long pos, String str) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * [Not supported] Sets a substring.
     */
    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        throw unsupported("LOB update");
    }

    /**
     * [Not supported] Searches a pattern and return the position.
     */
    @Override
    public long position(String pattern, long start) throws SQLException {
        throw unsupported("LOB search");
    }

    /**
     * [Not supported] Searches a pattern and return the position.
     */
    @Override
    public long position(Clob clobPattern, long start) throws SQLException {
        throw unsupported("LOB search");
    }

    /**
     * Release all resources of this object.
     */
    @Override
    public void free() {
        debugCodeCall("free");
        value = null;
    }

    /**
     * [Not supported] Returns the reader, starting from an offset.
     */
    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        throw unsupported("LOB subset");
    }

    private void checkClosed() {
        if (value == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": " + (value == null ? "null" : value.getTraceSQL());
    }
}

/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.lob;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.zip.ZipOutputStream;

import org.lealone.db.value.ValueLob;
import org.lealone.transaction.Transaction;

/**
 * A mechanism to store and retrieve lob data.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface LobStorage {

    /**
     * The table id for session variables (LOBs not assigned to a table).
     */
    int TABLE_ID_SESSION_VARIABLE = -1;

    /**
     * The table id for temporary objects (not assigned to any object).
     */
    int TABLE_TEMP = -2;

    default void save() {
    }

    default void gc(ConcurrentSkipListMap<Long, ? extends Transaction> currentTransactions) {
    }

    default void close() {
    }

    default void backupTo(String baseDir, ZipOutputStream out, Long lastDate) {
    }

    /**
     * Create a BLOB object.
     *
     * @param in the input stream
     * @param maxLength the maximum length (-1 if not known)
     * @return the LOB
     */
    ValueLob createBlob(InputStream in, long maxLength);

    /**
     * Create a CLOB object.
     *
     * @param reader the reader
     * @param maxLength the maximum length (-1 if not known)
     * @return the LOB
     */
    ValueLob createClob(Reader reader, long maxLength);

    /**
     * Get the input stream for the given lob.
     *
     * @param lob the lob id
     * @param hmac the message authentication code (for remote input streams)
     * @param byteCount the number of bytes to read, or -1 if not known
     * @return the stream
     */
    InputStream getInputStream(ValueLob lob, byte[] hmac, long byteCount) throws IOException;

    /**
     * Set the table reference of this lob.
     *
     * @param lob the lob
     * @param table the table
     */
    void setTable(ValueLob lob, int table);

    /**
     * Remove all LOBs for this table.
     *
     * @param tableId the table id
     */
    void removeAllForTable(int tableId);

    /**
     * Delete a LOB (from the database, if it is stored there).
     *
     * @param lob the lob
     */
    void removeLob(ValueLob lob);

}

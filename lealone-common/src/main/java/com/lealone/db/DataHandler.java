/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.TempFileDeleter;
import com.lealone.storage.fs.FileStorage;
import com.lealone.storage.lob.LobStorage;

/**
 * A data handler contains a number of callback methods.
 * The most important implementing class is a database.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface DataHandler {

    /**
     * Get the database path.
     *
     * @return the database path
     */
    String getDatabasePath();

    /**
     * Open a file at the given location.
     *
     * @param name the file name
     * @param mode the mode
     * @param mustExist whether the file must already exist
     * @return the file
     */
    FileStorage openFile(String name, String mode, boolean mustExist);

    /**
     * Get the temp file deleter mechanism.
     *
     * @return the temp file deleter
     */
    TempFileDeleter getTempFileDeleter();

    /**
     * Check if the simulated power failure occurred.
     * This call will decrement the countdown.
     *
     * @throws DbException if the simulated power failure occurred
     */
    void checkPowerOff() throws DbException;

    /**
     * Check if writing is allowed.
     *
     * @throws DbException if it is not allowed
     */
    void checkWritingAllowed() throws DbException;

    /**
     * Get the maximum length of a in-place large object
     *
     * @return the maximum size
     */
    int getMaxLengthInplaceLob();

    /**
     * Get the compression algorithm used for large objects.
     *
     * @param type the data type (CLOB or BLOB)
     * @return the compression algorithm, or null
     */
    String getLobCompressionAlgorithm(int type);

    /**
     * Get the lob storage mechanism to use.
     *
     * @return the lob storage mechanism
     */
    LobStorage getLobStorage();

    default boolean isTableLobStorage() {
        return false;
    }
}

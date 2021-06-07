/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.common.security;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.api.ErrorCode;

/**
 * A factory to create new block cipher objects.
 */
public class CipherFactory {

    private CipherFactory() {
        // utility class
    }

    /**
     * Get a new block cipher object for the given algorithm.
     *
     * @param algorithm the algorithm
     * @return a new cipher object
     */
    public static BlockCipher getBlockCipher(String algorithm) {
        if ("XTEA".equalsIgnoreCase(algorithm)) {
            return new XTEA();
        } else if ("AES".equalsIgnoreCase(algorithm)) {
            return new AES();
        } else if ("FOG".equalsIgnoreCase(algorithm)) {
            return new Fog();
        }
        throw DbException.get(ErrorCode.UNSUPPORTED_CIPHER, algorithm);
    }
}

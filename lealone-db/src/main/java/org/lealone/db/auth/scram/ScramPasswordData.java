/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.auth.scram;

public class ScramPasswordData {

    public final byte[] salt;
    public final byte[] saltedPassword;
    public final byte[] storedKey;
    public final byte[] serverKey;
    public final int iterations;

    public ScramPasswordData(byte[] salt, byte[] saltedPassword, byte[] storedKey, byte[] serverKey,
            int iterations) {
        this.salt = salt;
        this.saltedPassword = saltedPassword;
        this.storedKey = storedKey;
        this.serverKey = serverKey;
        this.iterations = iterations;
    }
}

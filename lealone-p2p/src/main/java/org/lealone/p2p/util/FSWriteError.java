/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.util;

import java.io.File;
import java.io.IOError;

public class FSWriteError extends IOError {
    public final File path;

    public FSWriteError(Throwable cause, File path) {
        super(cause);
        this.path = path;
    }

    @Override
    public String toString() {
        return "FSWriteError in " + path;
    }
}

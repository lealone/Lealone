/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage;

public interface FormatVersion {

    public static final int FORMAT_VERSION_1 = 1;
    public static final int FORMAT_VERSION = 2;

    public static boolean isOldFormatVersion(int formatVersion) {
        return formatVersion == FORMAT_VERSION_1;
    }
}

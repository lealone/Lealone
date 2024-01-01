/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.storage.fs.impl.nio;

import java.io.IOException;
import java.nio.channels.FileChannel;

import com.lealone.storage.fs.impl.FilePathWrapper;

/**
 * This file system stores files on disk and uses java.nio to access the files.
 * This class uses FileChannel.
 */
public class FilePathNio extends FilePathWrapper {

    @Override
    public String getScheme() {
        return "nio";
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileNio(name.substring(getScheme().length() + 1), mode);
    }
}

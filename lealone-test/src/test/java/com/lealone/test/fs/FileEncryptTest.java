/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.fs;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Test;

import com.lealone.storage.fs.FilePath;
import com.lealone.storage.fs.FileUtils;
import com.lealone.storage.fs.impl.encrypt.FilePathEncrypt;

public class FileEncryptTest extends FsTestBase {
    @Test
    public void run() throws Exception {
        FilePathEncrypt.register();

        String fileName = "encrypt:mypassword:" + joinDirs("FsTest", "FileEncryptTest.dat");
        FilePath fp = FilePath.get(fileName);
        if (!fp.exists()) {
            FileUtils.createDirectories(joinDirs("FsTest"));
            fp.createFile();
        }
        FileChannel file = fp.open("rw");
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            buffer.append("1234567890");
        }
        byte[] bytes = buffer.toString().getBytes();
        file.write(ByteBuffer.wrap(bytes));
        file.close();

        fp = FilePath.get(fileName);
        file = fp.open("rw");
        ByteBuffer bb = ByteBuffer.allocate(bytes.length);
        file.read(bb);
        bb.flip();

        assertTrue(buffer.toString().equals(new String(bb.array())));
        file.close();
        fp.delete();
    }
}

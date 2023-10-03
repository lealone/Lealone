/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.fs;

import java.io.File;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.lealone.common.security.SHA256;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileStorageInputStream;
import org.lealone.storage.fs.FileStorageOutputStream;
import org.lealone.test.TestBase;

public class FileStorageTest extends FsTestBase {

    public static String joinDirs(String... dirs) {
        String path = TestBase.joinDirs(dirs);
        File dir = new File(path);
        if (!dir.getParentFile().exists())
            dir.getParentFile().mkdirs();
        return path;
    }

    @Test
    public void run() {
        FileStorage fs = FileStorage.open(null, "nio:./src/test/resources/lealone-test.yaml", "r");
        assertTrue(fs.length() > 0);
    }

    @Test
    public void testEncrypt() {
        byte[] keyHash = SHA256.getKeyPasswordHash("FileStorageTest", "mypassword".toCharArray());
        String fileName = joinDirs("FileStorageTest", "EncryptFileStorageTest.dat");
        FileStorage fs = FileStorage.open(null, fileName, "rw", "XTEA", keyHash);

        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            buffer.append("1234567890");
        }
        byte[] bytes = buffer.toString().getBytes();
        fs.writeFully(0, ByteBuffer.wrap(bytes));
        fs.close();

        // 在SHA256.getPBKDF2(byte[], byte[], int, int)中会清空keyHash
        keyHash = SHA256.getKeyPasswordHash("FileStorageTest", "mypassword".toCharArray());
        fs = FileStorage.open(null, fileName, "r", "XTEA", keyHash);
        ByteBuffer bb = fs.readFully(0, bytes.length);
        assertTrue(buffer.toString().equals(new String(bb.array(), bb.arrayOffset(), bytes.length)));
        fs.close();
    }

    @Test
    public void testStream() throws Exception {
        testStream("LZF");
        testStream(null);
    }

    private void testStream(String compressionAlgorithm) throws Exception {
        boolean compression = compressionAlgorithm != null;
        String fileName = "FileStorageStreamTest.dat";
        if (compression)
            fileName = compressionAlgorithm + fileName;
        fileName = joinDirs("FileStorageTest", fileName);
        FileStorage fs = FileStorage.open(null, fileName, "rw");
        FileStorageOutputStream out = new FileStorageOutputStream(fs, null, compressionAlgorithm);
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            buffer.append("1234567890");
        }
        byte[] bytes = buffer.toString().getBytes();
        out.write(bytes);
        String str = "abc1234567890def";
        out.write(str.getBytes());
        out.close();
        fs.close();

        byte[] bytes2 = new byte[bytes.length];
        fs = FileStorage.open(null, fileName, "r");
        FileStorageInputStream in = new FileStorageInputStream(fs, null, compression, false);
        in.read(bytes2);
        assertTrue(buffer.toString().equals(new String(bytes2)));
        bytes2 = new byte[str.getBytes().length];
        in.read(bytes2);
        assertTrue(str.equals(new String(bytes2)));
        in.close();
        fs.close();
    }
}

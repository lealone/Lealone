/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.fs;

import org.junit.Test;
import org.lealone.storage.fs.FilePath;

public class FilePathTest extends FsTestBase {
    @Test
    public void run() {
        FilePath fp = FilePath.get("nio:./src/test/resources/lealone-test.yaml");
        assertTrue(fp.exists());

        fp = FilePath.get("file:./src/test/resources/lealone-test.yaml");
        assertTrue(fp.exists());
    }
}

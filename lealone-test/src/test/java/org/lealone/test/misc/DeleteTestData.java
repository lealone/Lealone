/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc;

import java.io.File;
import java.io.IOException;

import org.lealone.storage.fs.FileUtils;
import org.lealone.test.TestBase;

public class DeleteTestData {

    public static void main(String[] args) throws IOException {
        FileUtils.deleteRecursive(TestBase.TEST_BASE_DIR, true);
        if (!FileUtils.exists(TestBase.TEST_BASE_DIR)) {
            System.out.println("dir '" + new File(TestBase.TEST_BASE_DIR).getCanonicalPath() + "' deleted");
        }
    }

}

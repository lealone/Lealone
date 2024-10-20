/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.misc;

import java.io.File;
import java.io.IOException;

import com.lealone.storage.fs.FileUtils;
import com.lealone.test.TestBase;

public class DeleteRedoLog {

    public static void main(String[] args) throws IOException {
        String redoLogDir = TestBase.TEST_BASE_DIR + File.separatorChar + "redo_log";
        FileUtils.deleteRecursive(redoLogDir, true);
        if (!FileUtils.exists(redoLogDir)) {
            System.out.println("dir '" + new File(redoLogDir).getCanonicalPath() + "' deleted");
        }
    }

}

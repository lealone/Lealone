/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.misc;

import java.io.File;
import java.io.IOException;

import com.lealone.main.Lealone;
import com.lealone.sql.config.Config;
import com.lealone.storage.fs.FileUtils;
import com.lealone.test.TestBase;

public class DeleteTestData {

    public static void main(String[] args) throws IOException {
        Config c = Lealone.createConfig();
        String dir = c.base_dir != null ? c.base_dir : TestBase.TEST_BASE_DIR;
        FileUtils.deleteRecursive(dir, true);
        if (!FileUtils.exists(dir)) {
            System.out.println("dir '" + new File(dir).getCanonicalPath() + "' deleted");
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

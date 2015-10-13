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
package org.lealone.test.storage;

import org.lealone.storage.memory.MemoryStorageEngine;
import org.lealone.test.TestBase;
import org.lealone.test.misc.CRUDExample;

public class MemoryStorageTest {

    public static void main(String[] args) throws Exception {
        // 会自动加载src/test/resource/META-INF/services/org.lealone.storage.StorageEngine中的类，
        // 这样的调用是多于的，会生成两个MemoryStorageEngine实例，不过StorageEngineManager只保留其中之一。
        // StorageEngineManager.registerStorageEngine(new MemoryStorageEngine());

        TestBase test = new TestBase();
        test.setStorageEngineName(MemoryStorageEngine.NAME);
        test.setEmbedded(true);
        test.printURL();

        CRUDExample.crud(test.getConnection());
    }

}

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
package org.lealone.transaction;

import java.lang.Thread.UncaughtExceptionHandler;

import org.lealone.engine.Constants;
import org.lealone.engine.Session;
import org.lealone.engine.SysProperties;
import org.lealone.mvstore.MVStore;
import org.lealone.mvstore.MVStoreCache;
import org.lealone.mvstore.MVStoreTool;
import org.lealone.store.fs.FileUtils;

public class TransactionManager {
    private static MVStore store;

    public static synchronized void init(String baseDir) {
        if (store != null)
            return;
        initStore(baseDir);

        TransactionStatusTable.init(store);
        TimestampServiceTable.init(store);
    }

    private static void initStore(String baseDir) {
        if (baseDir == null) {
            baseDir = SysProperties.getBaseDir();
        }

        String fileName = FileUtils.toRealPath(baseDir + "/system") + Constants.SUFFIX_MV_FILE;

        if (MVStoreCache.getMVStore(fileName) != null) {
            store = MVStoreCache.getMVStore(fileName);
            return;
        }
        MVStoreTool.compactCleanUp(fileName);
        MVStore.Builder builder = new MVStore.Builder();
        builder.fileName(fileName);

        // possibly create the directory
        boolean exists = FileUtils.exists(fileName);
        if (exists && !FileUtils.canWrite(fileName)) {
            // read only
        } else {
            String dir = FileUtils.getParent(fileName);
            FileUtils.createDirectories(dir);
        }
        builder.backgroundExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                //TODO
            }
        });

        store = builder.open();
    }

    public static Transaction beginTransaction(Session session) {
        if (session.isLocal())
            return new LocalTransaction(session);
        else
            return new GlobalTransaction(session);
    }
}

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
package org.lealone.storage.replication;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

class ReadResponseHandler<T> extends ReplicationHandler<T> {

    private final int r;

    ReadResponseHandler(int n, AsyncHandler<AsyncResult<T>> topHandler) {
        super(n, topHandler);
        // r = n / 2 + 1;
        r = 1; // 使用Write all read one模式
    }

    @Override
    synchronized void response(AsyncResult<T> result) {
        results.add(result);
        if (!successful && results.size() >= r) {
            successful = true;
            signal();
            if (topHandler != null) {
                topHandler.handle(results.get(0));
            }
        }
    }

    @Override
    boolean isRead() {
        return true;
    }

    @Override
    int totalBlockFor() {
        return r;
    }
}

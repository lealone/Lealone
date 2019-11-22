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

import java.util.ArrayList;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

class WriteResponseHandler<T> extends ReplicationHandler<T> {

    private final int w;
    private final ReplicationResult replicationResult;

    WriteResponseHandler(int n, AsyncHandler<AsyncResult<T>> topHandler, ReplicationResult replicationResult) {
        super(n, topHandler);
        // w = n / 2 + 1;
        w = n; // 使用Write all read one模式
        this.replicationResult = replicationResult;
    }

    @Override
    @SuppressWarnings("unchecked")
    synchronized void response(AsyncResult<T> result) {
        results.add(result);
        if (!successful && results.size() >= w) {
            successful = true;
            signal();
            Object ret = null;
            if (replicationResult != null) {
                ret = replicationResult.validate(getResults());
            }
            if (topHandler != null) {
                if (ret != null)
                    topHandler.handle((AsyncResult<T>) ret);
                else
                    topHandler.handle(results.get(0));
            }
        }
    }

    @Override
    boolean isRead() {
        return false;
    }

    @Override
    int totalBlockFor() {
        return w;
    }

    ArrayList<T> getResults() {
        int size = results.size();
        ArrayList<T> results2 = new ArrayList<>(results.size());
        for (int i = 0; i < size; i++)
            results2.add(results.get(i).getResult());
        return results2;
    }
}

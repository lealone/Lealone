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
package org.lealone.transaction.amte.log;

import java.util.Map;

import org.lealone.transaction.amte.AMTransaction;

class NoLogSyncService extends LogSyncService {

    NoLogSyncService(Map<String, String> config) {
        super(config);
    }

    @Override
    public void start() {
    }

    @Override
    public void run() {
    }

    @Override
    public boolean needSync() {
        return false;
    }

    @Override
    public void maybeWaitForSync(RedoLogRecord r) {
    }

    @Override
    public void prepareCommit(AMTransaction t) {
        if (t.getSession() != null) {
            t.getSession().commit(null);
        }
    }
}

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
package org.lealone.mvstore.mvcc;

class VersionedValue {

    public final long tid;
    public final int logId;
    public final Object value;

    public VersionedValue(Object value) {
        this(0, 0, value);
    }

    public VersionedValue(long tid, int logId, Object value) {
        this.tid = tid;
        this.logId = logId;
        this.value = value;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder("VersionedValue[ ");
        buff.append("tid = ").append(tid);
        buff.append(", logId = ").append(logId);
        buff.append(", value = ").append(value).append(" ]");
        return buff.toString();
    }

}

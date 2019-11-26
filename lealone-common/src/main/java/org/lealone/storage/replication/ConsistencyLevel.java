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

public enum ConsistencyLevel {
    ALL(0),
    QUORUM(1),
    LOCAL_QUORUM(2, true),
    EACH_QUORUM(3);

    public final int code;
    private final boolean isDCLocal;

    private ConsistencyLevel(int code) {
        this(code, false);
    }

    private ConsistencyLevel(int code, boolean isDCLocal) {
        this.code = code;
        this.isDCLocal = isDCLocal;
    }

    public boolean isDatacenterLocal() {
        return isDCLocal;
    }

    public static ConsistencyLevel getLevel(String code) {
        return getLevel(Integer.parseInt(code));
    }

    public static ConsistencyLevel getLevel(int code) {
        switch (code) {
        case 0:
            return ConsistencyLevel.ALL;
        case 1:
            return ConsistencyLevel.QUORUM;
        case 2:
            return ConsistencyLevel.LOCAL_QUORUM;
        case 3:
            return ConsistencyLevel.EACH_QUORUM;
        default:
            return ConsistencyLevel.ALL;
        }
    }
}

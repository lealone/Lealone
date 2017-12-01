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
package org.lealone.mvcc.log;

import java.nio.ByteBuffer;

//RedoLog文件中会有三种类型的日志条目
public class RedoLogValue {
    // 1. 本地事务只包含这个字段
    public ByteBuffer values;

    // 2. 分布式事务多加这三个字段
    public String transactionName;
    public String allLocalTransactionNames;
    public long commitTimestamp;

    // 3. 检查点只有这个字段
    public Long checkpoint;

    // 4. 已经被删除的map
    public String droppedMap;

    volatile boolean synced;

    public RedoLogValue() {
    }

    public RedoLogValue(ByteBuffer values) {
        this.values = values;
    }

    public RedoLogValue(Long checkpoint) {
        this.checkpoint = checkpoint;
    }

    public RedoLogValue(String droppedMap) {
        this.droppedMap = droppedMap;
    }
}

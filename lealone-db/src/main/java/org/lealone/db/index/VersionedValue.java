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
package org.lealone.db.index;

import org.lealone.db.value.ValueArray;

public class VersionedValue {

    public final int vertion; // 表的元数据版本号
    public final ValueArray value;

    public VersionedValue(int vertion, ValueArray value) {
        this.vertion = vertion;
        this.value = value;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder("VersionedValue[ ");
        buff.append("vertion = ").append(vertion);
        buff.append(", value = ").append(value).append(" ]");
        return buff.toString();
    }

}
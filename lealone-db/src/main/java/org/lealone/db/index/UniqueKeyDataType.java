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

import org.lealone.db.DataHandler;
import org.lealone.db.value.CompareMode;

/**
 * 用于优化唯一性检查，包括唯一约束、多字段primary key以及非byte/short/int/long类型的单字段primary key
 * 通过SecondaryIndex增加索引记录时不需要在执行addIfAbsent前后做唯一性检查
 */
public class UniqueKeyDataType extends ValueDataType {

    public UniqueKeyDataType(DataHandler handler, CompareMode compareMode, int[] sortTypes) {
        super(handler, compareMode, sortTypes);
    }

    @Override
    protected boolean isUniqueKey() {
        return true;
    }
}

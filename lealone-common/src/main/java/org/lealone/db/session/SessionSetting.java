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
package org.lealone.db.session;

/**
 * The list of session setting for a SET statement.
 */
// session级的只要更新当前session的状态即可
// 除了VARIABLE枚举常量使用SET @VARIABLE这样的特殊语法外，其他的都直接在SET后面接枚举常量名
public enum SessionSetting {

    LOCK_TIMEOUT,
    QUERY_TIMEOUT,
    SCHEMA,
    SCHEMA_SEARCH_PATH,
    VARIABLE,
    THROTTLE;

    public String getName() {
        if (this == VARIABLE)
            return "@";
        else
            return name();
    }

    public static boolean contains(String name) {
        try {
            SessionSetting.valueOf(name);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}

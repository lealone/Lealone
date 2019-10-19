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
package org.lealone.common.trace;

public enum TraceModuleType {

    /**
     * The trace module type for commands.
     */
    COMMAND,

    /**
     * The trace module type for constraints.
     */
    CONSTRAINT,

    /**
     * The trace module type for databases.
     */
    DATABASE,

    /**
     * The trace module type for functions.
     */
    FUNCTION,

    /**
     * The trace module type for indexes.
     */
    INDEX,

    /**
     * The trace module type for the JDBC API.
     */
    JDBC,

    /**
     * The trace module type for the JDBCX API
     */
    JDBCX,

    /**
     * The trace module type for locks.
     */
    LOCK,

    /**
     * The trace module type for schemas.
     */
    SCHEMA,

    /**
     * The trace module type for sequences.
     */
    SEQUENCE,

    /**
     * The trace module type for settings.
     */
    SETTING,

    /**
     * The trace module type for tables.
     */
    TABLE,

    /**
     * The trace module type for triggers.
     */
    TRIGGER,

    /**
     * The trace module type for users.
     */
    USER;
}

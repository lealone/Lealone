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

import org.lealone.common.util.CamelCaseHelper;

public enum TraceObjectType {

    /**
     * The trace type for connections.
     */
    CONNECTION("conn"),

    /**
     * The trace type for statements.
     */
    STATEMENT("stat"),

    /**
     * The trace type for prepared statements.
     */
    PREPARED_STATEMENT("ps"),

    /**
     * The trace type for callable statements.
     */
    CALLABLE_STATEMENT("call"),

    /**
     * The trace type for result sets.
     */
    RESULT_SET("rs"),

    /**
     * The trace type for result set meta data objects.
     */
    RESULT_SET_META_DATA("rsMeta"),

    /**
     * The trace type for parameter meta data objects.
     */
    PARAMETER_META_DATA("pMeta"),

    /**
     * The trace type for database meta data objects.
     */
    DATABASE_META_DATA("dbMeta"),

    /**
     * The trace type for savepoint objects.
     */
    SAVEPOINT("sp"),

    /**
     * The trace type for blobs.
     */
    BLOB("blob"),

    /**
     * The trace type for clobs.
     */
    CLOB("clob"),

    /**
     * The trace type for array objects.
     */
    ARRAY("ar"),

    /**
     * The trace type for data sources.
     */
    DATA_SOURCE("ds");

    private final String shortName;
    private final String className;

    TraceObjectType(String shortName) {
        this.shortName = shortName;
        className = CamelCaseHelper.toClassNameFromUnderscore(name());
    }

    public String getShortName() {
        return shortName;
    }

    public String getClassName() {
        return className;
    }
}

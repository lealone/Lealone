/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
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

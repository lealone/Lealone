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
package org.lealone.db.expression;

public class Comparison {

    /**
     * This is a flag meaning the comparison is null safe (meaning never returns
     * NULL even if one operand is NULL). Only EQUAL and NOT_EQUAL are supported
     * currently.
     */
    public static final int NULL_SAFE = 16;

    /**
     * The comparison type meaning = as in ID=1.
     */
    public static final int EQUAL = 0;

    /**
     * The comparison type meaning ID IS 1 (ID IS NOT DISTINCT FROM 1).
     */
    public static final int EQUAL_NULL_SAFE = EQUAL | NULL_SAFE;

    /**
     * The comparison type meaning &gt;= as in ID&gt;=1.
     */
    public static final int BIGGER_EQUAL = 1;

    /**
     * The comparison type meaning &gt; as in ID&gt;1.
     */
    public static final int BIGGER = 2;

    /**
     * The comparison type meaning &lt;= as in ID&lt;=1.
     */
    public static final int SMALLER_EQUAL = 3;

    /**
     * The comparison type meaning &lt; as in ID&lt;1.
     */
    public static final int SMALLER = 4;

    /**
     * The comparison type meaning &lt;&gt; as in ID&lt;&gt;1.
     */
    public static final int NOT_EQUAL = 5;

    /**
     * The comparison type meaning ID IS NOT 1 (ID IS DISTINCT FROM 1).
     */
    public static final int NOT_EQUAL_NULL_SAFE = NOT_EQUAL | NULL_SAFE;

    /**
     * The comparison type meaning IS NULL as in NAME IS NULL.
     */
    public static final int IS_NULL = 6;

    /**
     * The comparison type meaning IS NOT NULL as in NAME IS NOT NULL.
     */
    public static final int IS_NOT_NULL = 7;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means the comparison will always yield FALSE. Example: 1=0.
     */
    public static final int FALSE = 8;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means equals any value of a list. Example: IN(1, 2, 3).
     */
    public static final int IN_LIST = 9;

    /**
     * This is a pseudo comparison type that is only used for index conditions.
     * It means equals any value of a list. Example: IN(SELECT ...).
     */
    public static final int IN_QUERY = 10;

}

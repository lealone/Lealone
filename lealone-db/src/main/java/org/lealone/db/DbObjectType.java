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
package org.lealone.db;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public enum DbObjectType {

    /**
     * The object is a database
     */
    DATABASE(0, 0),

    /**
     * This object is a user.
     */
    USER(1, 2),

    /**
     * This object is a role.
     */
    ROLE(2, 12),

    /**
     * This object is a right.
     */
    RIGHT(3, 13),

    /**
     * This object is a user data type (domain).
     */
    USER_DATATYPE(4, 5),

    /**
     * This object is a user-defined aggregate function.
     */
    AGGREGATE(5, 14),

    /**
     * This object is a setting.
     */
    SETTING(6, 1),

    /**
     * This object is a comment.
     */
    COMMENT(7, 15),

    /**
     * This object is a schema.
     */
    SCHEMA(20, 3),

    /**
     * The object is of the type table or view.
     */
    TABLE_OR_VIEW(21, 8),

    /**
     * This object is an index.
     */
    INDEX(22, 9),

    /**
     * This object is a sequence.
     */
    SEQUENCE(23, 6),

    /**
     * This object is a trigger.
     */
    TRIGGER(24, 11),

    /**
     * This object is a constraint (check constraint, unique constraint, or
     * referential constraint).
     */
    CONSTRAINT(25, 10),

    /**
     * This object is an alias for a Java function.
     */
    FUNCTION_ALIAS(26, 4),

    /**
     * This object is a constant.
     */
    CONSTANT(27, 7),

    /**
     * This object is a service.
     */
    SERVICE(28, 100),

    __LAST__(29, 1000);

    public final int value; // 值被存入SYS表中，不能随意改动

    /**
     * Objects are created in this order when opening a database.
     */
    public final int createOrder;

    private DbObjectType(int value, int createOrder) {
        this.value = value;
        this.createOrder = createOrder;
    }

    public static final DbObjectType[] TYPES = new DbObjectType[__LAST__.value + 1];
    static {
        for (DbObjectType t : DbObjectType.values())
            TYPES[t.value] = t;
    }

    public static void main(String[] args) {
        List<DbObjectType> list = Arrays.asList(DbObjectType.values());
        Collections.sort(list, new Comparator<DbObjectType>() {
            @Override
            public int compare(DbObjectType o1, DbObjectType o2) {
                return o1.createOrder - o2.createOrder;
            }

        });
        for (DbObjectType t : list)
            System.out.println(t);
    }

}

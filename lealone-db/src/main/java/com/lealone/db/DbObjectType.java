/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public enum DbObjectType {

    /**
     * The object is a database
     */
    DATABASE(0, 0, false),

    /**
     * This object is a user.
     */
    USER(1, 2, false),

    /**
     * This object is a role.
     */
    ROLE(2, 12, false),

    /**
     * This object is a right.
     */
    RIGHT(3, 13, false),

    /**
     * This object is a user data type (domain).
     */
    USER_DATATYPE(4, 5),

    /**
     * This object is a user-defined aggregate function.
     */
    AGGREGATE(5, 14),

    /**
     * This object is a comment.
     */
    COMMENT(6, 15, false),

    /**
     * This object is a schema.
     */
    SCHEMA(7, 3, false),

    /**
     * The object is of the type table or view.
     */
    TABLE_OR_VIEW(8, 8),

    /**
     * This object is an index.
     */
    INDEX(9, 9),

    /**
     * This object is a sequence.
     */
    SEQUENCE(10, 6),

    /**
     * This object is a trigger.
     */
    TRIGGER(11, 11),

    /**
     * This object is a constraint (check constraint, unique constraint, or
     * referential constraint).
     */
    CONSTRAINT(12, 10),

    /**
     * This object is an alias for a Java function.
     */
    FUNCTION_ALIAS(13, 4),

    /**
     * This object is a constant.
     */
    CONSTANT(14, 7),

    /**
     * This object is a service.
     */
    SERVICE(15, 100),

    /**
     * This object is a plugin.
     */
    PLUGIN(16, 3, false); // PLUGIN的优先级要高一些

    private static final int MAX = 17;

    public final int value; // 值被存入SYS表中，不能随意改动

    /**
     * Objects are created in this order when opening a database.
     */
    public final int createOrder;
    public final boolean isSchemaObject;

    private DbObjectType(int value, int createOrder) {
        this(value, createOrder, true);
    }

    private DbObjectType(int value, int createOrder, boolean isSchemaObject) {
        this.value = value;
        this.createOrder = createOrder;
        this.isSchemaObject = isSchemaObject;
    }

    public static final DbObjectType[] TYPES = new DbObjectType[MAX];
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

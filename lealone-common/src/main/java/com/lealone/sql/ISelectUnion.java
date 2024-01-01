/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql;

import com.lealone.db.result.Result;

public interface ISelectUnion {

    /**
    * The type of a UNION statement.
    */
    public static final int UNION = 0;

    /**
    * The type of a UNION ALL statement.
    */
    public static final int UNION_ALL = 1;

    /**
    * The type of an EXCEPT statement.
    */
    public static final int EXCEPT = 2;

    /**
    * The type of an INTERSECT statement.
    */
    public static final int INTERSECT = 3;

    int getUnionType();

    IQuery getLeft();

    IQuery getRight();

    Result getEmptyResult();

}

/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector;

public class ValueVectorArray extends ValueVector {

    // private ValueVector[] a;

    public ValueVectorArray(ValueVector[] a) {
        // this.a = a;
    }

    @Override
    public BooleanVector compare(ValueVector vv, int compareType) {
        // ValueVectorArray v = (ValueVectorArray) vv;
        return null;
    }
}

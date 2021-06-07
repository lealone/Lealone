/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.util;

public class Pair<T1, T2> {
    public final T1 left;
    public final T2 right;

    protected Pair(T1 left, T2 right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public final int hashCode() {
        int hashCode = 31 + (left == null ? 0 : left.hashCode());
        return 31 * hashCode + (right == null ? 0 : right.hashCode());
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof Pair))
            return false;
        Pair<?, ?> that = (Pair<?, ?>) o;
        // handles nulls properly
        return equal(left, that.left) && equal(right, that.right);
    }

    @Override
    public String toString() {
        return "(" + left + "," + right + ")";
    }

    public static <X, Y> Pair<X, Y> create(X x, Y y) {
        return new Pair<X, Y>(x, y);
    }

    private static boolean equal(Object a, Object b) {
        return a == b || (a != null && a.equals(b));
    }
}

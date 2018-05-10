package org.lealone.orm.typequery;

/**
 * Base property for all comparable types. 
 *
 * @param <R> the root query bean type
 * @param <T> the type of the scalar property
 */
@SuppressWarnings("rawtypes")
public class PBaseComparable<R, T extends Comparable> extends PBaseValueEqual<R, T> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PBaseComparable(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PBaseComparable(String name, R root, String prefix) {
        super(name, root, prefix);
    }

    // ---- range comparisons -------
    /**
     * Greater than.
     *
     * @param value the bind value
     * @return the root query bean instance
     */
    public final R gt(T value) {
        expr().gt(name, value);
        return root;
    }

    /**
     * Greater than or Equal to.
     *
     * @param value the bind value
     * @return the root query bean instance
     */
    public final R ge(T value) {
        expr().ge(name, value);
        return root;
    }

    /**
     * Less than.
     *
     * @param value the bind value
     * @return the root query bean instance
     */
    public final R lt(T value) {
        expr().lt(name, value);
        return root;
    }

    /**
     * Less than or Equal to.
     *
     * @param value the bind value
     * @return the root query bean instance
     */
    public final R le(T value) {
        expr().le(name, value);
        return root;
    }

    /**
     * Between lower and upper values.
     *
     * @param lower the lower bind value
     * @param upper the upper bind value
     * @return the root query bean instance
     */
    public final R between(T lower, T upper) {
        expr().between(name, lower, upper);
        return root;
    }

    /**
     * Greater than.
     *
     * @param value the bind value
     * @return the root query bean instance
     */
    public final R greaterThan(T value) {
        expr().gt(name, value);
        return root;
    }

    /**
     * Greater than or Equal to.
     *
     * @param value the bind value
     * @return the root query bean instance
     */
    public final R greaterOrEqualTo(T value) {
        expr().ge(name, value);
        return root;
    }

    /**
     * Less than.
     *
     * @param value the bind value
     * @return the root query bean instance
     */
    public final R lessThan(T value) {
        expr().lt(name, value);
        return root;
    }

    /**
     * Less than or Equal to.
     *
     * @param value the bind value
     * @return the root query bean instance
     */
    public final R lessOrEqualTo(T value) {
        expr().le(name, value);
        return root;
    }

}

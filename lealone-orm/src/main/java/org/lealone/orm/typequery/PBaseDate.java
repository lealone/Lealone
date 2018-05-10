package org.lealone.orm.typequery;

/**
 * Base property for date and date time types.
 *
 * @param <R> the root query bean type
 * @param <D> the date time type
 */
@SuppressWarnings("rawtypes")
public abstract class PBaseDate<R, D extends Comparable> extends PBaseComparable<R, D> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PBaseDate(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PBaseDate(String name, R root, String prefix) {
        super(name, root, prefix);
    }

    /**
     * Same as greater than.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R after(D value) {
        expr().gt(name, value);
        return root;
    }

    /**
     * Same as less than.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R before(D value) {
        expr().lt(name, value);
        return root;
    }
}

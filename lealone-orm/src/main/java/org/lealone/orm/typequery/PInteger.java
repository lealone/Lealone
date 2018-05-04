package org.lealone.orm.typequery;

/**
 * Integer property.
 *
 * @param <R> the root query bean type
 */
public class PInteger<R> extends PBaseNumber<R, Integer> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PInteger(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PInteger(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

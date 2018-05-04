package org.lealone.orm.typequery;

/**
 * Double property.
 *
 * @param <R> the root query bean type
 */
public class PDouble<R> extends PBaseNumber<R, Double> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PDouble(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PDouble(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

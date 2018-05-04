package org.lealone.orm.typequery;

/**
 * Long property.
 *
 * @param <R> the root query bean type
 */
public class PLong<R> extends PBaseNumber<R, Long> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PLong(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PLong(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

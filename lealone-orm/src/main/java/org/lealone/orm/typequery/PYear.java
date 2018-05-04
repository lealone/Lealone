package org.lealone.orm.typequery;

import java.time.Year;

/**
 * Year property.
 *
 * @param <R> the root query bean type
 */
public class PYear<R> extends PBaseNumber<R, Year> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PYear(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PYear(String name, R root, String prefix) {
        super(name, root, prefix);
    }
}

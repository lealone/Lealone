package org.lealone.orm.typequery;

import java.util.TimeZone;

/**
 * TimeZone property.
 *
 * @param <R> the root query bean type
 */
public class PTimeZone<R> extends PBaseValueEqual<R, TimeZone> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PTimeZone(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PTimeZone(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

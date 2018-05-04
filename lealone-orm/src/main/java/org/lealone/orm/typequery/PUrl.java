package org.lealone.orm.typequery;

import java.net.URL;

/**
 * URL property.
 *
 * @param <R> the root query bean type
 */
public class PUrl<R> extends PBaseValueEqual<R, URL> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PUrl(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PUrl(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

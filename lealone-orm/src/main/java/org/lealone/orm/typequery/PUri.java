package org.lealone.orm.typequery;

import java.net.URI;

/**
 * URI property.
 *
 * @param <R> the root query bean type
 */
public class PUri<R> extends PBaseValueEqual<R, URI> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PUri(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PUri(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

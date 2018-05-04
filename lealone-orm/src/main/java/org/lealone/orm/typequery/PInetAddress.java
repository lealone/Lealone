package org.lealone.orm.typequery;

import java.net.InetAddress;

/**
 * InetAddress property.
 *
 * @param <R> the root query bean type
 */

public class PInetAddress<R> extends PBaseValueEqual<R, InetAddress> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PInetAddress(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PInetAddress(String name, R root, String prefix) {
        super(name, root, prefix);
    }
}

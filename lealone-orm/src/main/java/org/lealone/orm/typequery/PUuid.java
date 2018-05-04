package org.lealone.orm.typequery;

import java.util.UUID;

/**
 * UUID property.
 *
 * @param <R> the root query bean type
 */
public class PUuid<R> extends PBaseValueEqual<R, UUID> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PUuid(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PUuid(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

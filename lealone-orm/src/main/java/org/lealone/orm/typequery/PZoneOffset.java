package org.lealone.orm.typequery;

import java.time.ZoneOffset;

/**
 * ZoneOffset property.
 *
 * @param <R> the root query bean type
 */
public class PZoneOffset<R> extends PBaseValueEqual<R, ZoneOffset> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PZoneOffset(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PZoneOffset(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

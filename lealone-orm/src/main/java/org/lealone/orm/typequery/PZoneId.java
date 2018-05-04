package org.lealone.orm.typequery;

import java.time.ZoneId;

/**
 * ZoneId property.
 *
 * @param <R> the root query bean type
 */
public class PZoneId<R> extends PBaseValueEqual<R, ZoneId> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PZoneId(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PZoneId(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

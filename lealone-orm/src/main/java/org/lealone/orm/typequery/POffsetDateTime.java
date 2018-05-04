package org.lealone.orm.typequery;

import java.time.OffsetDateTime;

/**
 * OffsetDateTime property.
 *
 * @param <R> the root query bean type
 */
public class POffsetDateTime<R> extends PBaseNumber<R, OffsetDateTime> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public POffsetDateTime(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public POffsetDateTime(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

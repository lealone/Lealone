package org.lealone.orm.typequery;

import java.sql.Timestamp;

/**
 * Property for java sql Timestamp.
 *
 * @param <R> the root query bean type
 */
public class PTimestamp<R> extends PBaseDate<R, Timestamp> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PTimestamp(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PTimestamp(String name, R root, String prefix) {
        super(name, root, prefix);
    }
}

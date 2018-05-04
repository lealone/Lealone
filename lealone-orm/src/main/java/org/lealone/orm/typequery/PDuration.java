package org.lealone.orm.typequery;

import java.time.Duration;

/**
 * Duration property.
 *
 * @param <R> the root query bean type
 */
public class PDuration<R> extends PBaseNumber<R, Duration> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PDuration(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PDuration(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

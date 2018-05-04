package org.lealone.orm.typequery;

import java.time.LocalTime;

/**
 * LocalTime property.
 *
 * @param <R> the root query bean type
 */
public class PLocalTime<R> extends PBaseNumber<R, LocalTime> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PLocalTime(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PLocalTime(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

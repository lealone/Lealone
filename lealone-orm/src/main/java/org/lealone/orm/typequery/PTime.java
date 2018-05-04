package org.lealone.orm.typequery;

import java.sql.Time;

/**
 * Time property.
 *
 * @param <R> the root query bean type
 */
public class PTime<R> extends PBaseNumber<R, Time> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PTime(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PTime(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

package org.lealone.orm.typequery;

import java.time.DayOfWeek;

/**
 * DayOfWeek property.
 *
 * @param <R> the root query bean type
 */
public class PDayOfWeek<R> extends PBaseNumber<R, DayOfWeek> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PDayOfWeek(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PDayOfWeek(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

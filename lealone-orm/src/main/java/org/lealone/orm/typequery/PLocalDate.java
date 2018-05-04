package org.lealone.orm.typequery;

import java.time.LocalDate;

/**
 * LocalDate property.
 *
 * @param <R> the root query bean type
 */
public class PLocalDate<R> extends PBaseDate<R, LocalDate> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PLocalDate(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PLocalDate(String name, R root, String prefix) {
        super(name, root, prefix);
    }
}

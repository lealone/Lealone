package org.lealone.orm.typequery;

import java.time.Month;

/**
 * LocalDateTime property.
 *
 * @param <R> the root query bean type
 */
public class PMonth<R> extends PBaseDate<R, Month> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PMonth(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PMonth(String name, R root, String prefix) {
        super(name, root, prefix);
    }
}

package org.lealone.orm.typequery;

import java.time.LocalDateTime;

/**
 * LocalDateTime property.
 *
 * @param <R> the root query bean type
 */
public class PLocalDateTime<R> extends PBaseDate<R, LocalDateTime> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PLocalDateTime(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PLocalDateTime(String name, R root, String prefix) {
        super(name, root, prefix);
    }
}

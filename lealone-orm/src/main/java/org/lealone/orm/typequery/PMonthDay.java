package org.lealone.orm.typequery;

import java.time.MonthDay;

/**
 * MonthDay property.
 *
 * @param <R> the root query bean type
 */
public class PMonthDay<R> extends PBaseDate<R, MonthDay> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PMonthDay(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PMonthDay(String name, R root, String prefix) {
        super(name, root, prefix);
    }
}

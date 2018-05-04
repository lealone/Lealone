package org.lealone.orm.typequery;

import java.time.YearMonth;

/**
 * YearMonth property.
 *
 * @param <R> the root query bean type
 */
public class PYearMonth<R> extends PBaseDate<R, YearMonth> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PYearMonth(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PYearMonth(String name, R root, String prefix) {
        super(name, root, prefix);
    }
}

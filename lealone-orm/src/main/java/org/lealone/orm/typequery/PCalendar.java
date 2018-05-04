package org.lealone.orm.typequery;

import java.util.Calendar;

/**
 * Calendar property.
 *
 * @param <R> the root query bean type
 */
public class PCalendar<R> extends PBaseDate<R, Calendar> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PCalendar(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PCalendar(String name, R root, String prefix) {
        super(name, root, prefix);
    }
}

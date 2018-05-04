package org.lealone.orm.typequery;

import java.util.Date;

/**
 * Java util Date property.
 *
 * @param <R> the root query bean type
 */
public class PUtilDate<R> extends PBaseDate<R, Date> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PUtilDate(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PUtilDate(String name, R root, String prefix) {
        super(name, root, prefix);
    }
}

package org.lealone.orm.typequery;

import java.sql.Date;

/**
 * Java sql Date property.
 *
 * @param <R> the root query bean type
 */

public class PSqlDate<R> extends PBaseDate<R, Date> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PSqlDate(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PSqlDate(String name, R root, String prefix) {
        super(name, root, prefix);
    }
}

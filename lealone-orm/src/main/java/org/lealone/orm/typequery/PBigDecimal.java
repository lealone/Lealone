package org.lealone.orm.typequery;

import java.math.BigDecimal;

/**
 * BigDecimal property.
 * @param <R> the root query bean type
 */
public class PBigDecimal<R> extends PBaseNumber<R, BigDecimal> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PBigDecimal(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PBigDecimal(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

package org.lealone.orm.typequery;

import java.math.BigInteger;

/**
 * BigInteger property.
 *
 * @param <R> the root query bean type
 */
public class PBigInteger<R> extends PBaseNumber<R, BigInteger> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PBigInteger(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PBigInteger(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

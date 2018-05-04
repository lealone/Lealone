package org.lealone.orm.typequery;

import java.util.Currency;

/**
 * Currency property.
 *
 * @param <R> the root query bean type
 */
public class PCurrency<R> extends PBaseValueEqual<R, Currency> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PCurrency(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PCurrency(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

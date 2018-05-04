package org.lealone.orm.typequery;

/**
 * Byte property.
 *
 * @param <R> the root query bean type
 */
public class PByte<R> extends PBaseNumber<R, Byte> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PByte(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PByte(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

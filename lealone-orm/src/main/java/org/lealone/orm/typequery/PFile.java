package org.lealone.orm.typequery;

/**
 * File property.
 * <p>
 *   This is a placeholder in the sense that currently it has no supported expressions.
 * </p>
 *
 * @param <R> the root query bean type
 */
public class PFile<R> extends TQProperty<R> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PFile(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PFile(String name, R root, String prefix) {
        super(name, root, prefix);
    }

}

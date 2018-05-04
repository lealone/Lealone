package org.lealone.orm.typequery;

import org.lealone.orm.ExpressionList;
import org.lealone.orm.Query;

/**
 * A property used in type query.
 *
 * @param <R> The type of the owning root bean
 */
public class TQProperty<R> {

    protected final String name;

    protected final R root;

    /**
     * Construct with a property name and root instance.
     *
     * @param name the name of the property
     * @param root the root query bean instance
     */
    public TQProperty(String name, R root) {
        this(name, root, null);
    }

    /**
     * Construct with additional path prefix.
     */
    public TQProperty(String name, R root, String prefix) {
        this.root = root;
        name = TQPath.add(prefix, name);

        if (((Query<?, ?>) root).databaseToUpper()) {
            name = name.toUpperCase();
        }
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Internal method to return the underlying expression list.
     */
    protected ExpressionList<?> expr() {
        return ((Query<?, ?>) root).peekExprList();
    }

    /**
     * Is null.
     */
    public R isNull() {
        expr().isNull(name);
        return root;
    }

    /**
     * Is not null.
     */
    public R isNotNull() {
        expr().isNotNull(name);
        return root;
    }

    /**
     * Order by ascending on this property.
     */
    public R asc() {
        expr().orderBy().asc(name);
        return root;
    }

    /**
     * Order by descending on this property.
     */
    public R desc() {
        expr().orderBy().desc(name);
        return root;
    }

    /**
     * Return the property name.
     */
    public String propertyName() {
        return name;
    }

}

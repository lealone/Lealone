package org.lealone.orm.typequery;

/**
 * String property.
 *
 * @param <R> the root query bean type
 */
public class PString<R> extends PBaseCompareable<R, String> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PString(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PString(String name, R root, String prefix) {
        super(name, root, prefix);
    }

    /**
     * Case insensitive is equal to.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R ieq(String value) {
        expr().ieq(name, value);
        return root;
    }

    /**
     * Case insensitive is equal to.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R iequalTo(String value) {
        expr().ieq(name, value);
        return root;
    }

    /**
     * Like - include '%' and '_' placeholders as necessary.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R like(String value) {
        expr().like(name, value);
        return root;
    }

    /**
     * Starts with - uses a like with '%' wildcard added to the end.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R startsWith(String value) {
        expr().startsWith(name, value);
        return root;
    }

    /**
     * Ends with - uses a like with '%' wildcard added to the beginning.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R endsWith(String value) {
        expr().endsWith(name, value);
        return root;
    }

    /**
     * Contains - uses a like with '%' wildcard added to the beginning and end.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R contains(String value) {
        expr().contains(name, value);
        return root;
    }

    /**
     * Case insensitive like.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R ilike(String value) {
        expr().ilike(name, value);
        return root;
    }

    /**
     * Case insensitive starts with.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R istartsWith(String value) {
        expr().istartsWith(name, value);
        return root;
    }

    /**
     * Case insensitive ends with.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R iendsWith(String value) {
        expr().iendsWith(name, value);
        return root;
    }

    /**
     * Case insensitive contains.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R icontains(String value) {
        expr().icontains(name, value);
        return root;
    }

    /**
     * Add a full text "Match" expression.
     * <p>
     * This means the query will automatically execute against the document store (ElasticSearch).
     * </p>
     *
     * @param value the match expression
     */
    public R match(String value) {
        expr().match(name, value);
        return root;
    }
}

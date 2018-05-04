package org.lealone.orm.typequery;

/**
 * Array property with E as the element type.
 *
 * @param <R> the root query bean type
 * @param <E> the element type of the DbArray
 */
public class PArray<R, E> extends TQProperty<R> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PArray(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PArray(String name, R root, String prefix) {
        super(name, root, prefix);
    }

    /**
     * ARRAY contains the values.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.contains("4321")
     *    .findList();
     *
     * }</pre>
     *
     * @param values The values that should be contained in the array
     */
    @SafeVarargs
    public final R contains(E... values) {
        expr().arrayContains(name, (Object[]) values);
        return root;
    }

    /**
     * ARRAY does not contain the values.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.notContains("4321")
     *    .findList();
     *
     * }</pre>
     *
     * @param values The values that should not be contained in the array
     */
    @SafeVarargs
    public final R notContains(E... values) {
        expr().arrayNotContains(name, (Object[]) values);
        return root;
    }

    /**
     * ARRAY is empty.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.isEmpty()
     *    .findList();
     *
     * }</pre>
     */
    public R isEmpty() {
        expr().arrayIsEmpty(name);
        return root;
    }

    /**
     * ARRAY is not empty.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.isNotEmpty()
     *    .findList();
     *
     * }</pre>
     */
    public R isNotEmpty() {
        expr().arrayIsNotEmpty(name);
        return root;
    }

}

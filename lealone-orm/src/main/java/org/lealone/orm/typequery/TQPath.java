package org.lealone.orm.typequery;

/**
 * Helper for adding a path prefix to a property.
 */
public class TQPath {

    /**
     * Return the full path by adding the prefix to the property name (null safe).
     */
    public static String add(String prefix, String name) {
        return (prefix == null) ? name : prefix + "." + name;
    }
}

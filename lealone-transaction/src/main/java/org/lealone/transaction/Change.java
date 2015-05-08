/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction;

/**
 * A change in a map.
 */
class Change {

    /**
     * The name of the map where the change occurred.
     */
    public String mapName;

    /**
     * The key.
     */
    public Object key;

    /**
     * The value.
     */
    public Object value;
}
/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.aose.rtree;

import org.lealone.storage.aose.btree.BTreeMapBuilder;

/**
 * A builder for this class.
 */
public class RTreeMapBuilder<V> extends BTreeMapBuilder<SpatialKey, V> {

    private int dimensions = 2;

    /**
     * Create a new builder for maps with 2 dimensions.
     */
    public RTreeMapBuilder() {
        // default
    }

    /**
     * Set the dimensions.
     *
     * @param dimensions the dimensions to use
     * @return this
     */
    public RTreeMapBuilder<V> dimensions(int dimensions) {
        this.dimensions = dimensions;
        return this;
    }

    @Override
    public RTreeMap<V> openMap() {
        return new RTreeMap<V>(name, dimensions, valueType, config, aoStorage);
    }
}

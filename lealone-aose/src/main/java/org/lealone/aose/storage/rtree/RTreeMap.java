/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.aose.storage.rtree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.lealone.aose.storage.AOStorage;
import org.lealone.aose.storage.StorageMapBuilder;
import org.lealone.aose.storage.btree.BTreeMap;
import org.lealone.aose.storage.btree.BTreePage;
import org.lealone.aose.storage.btree.CursorPos;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.New;
import org.lealone.storage.type.StorageDataType;

/**
 * An r-tree implementation. It uses the quadratic split algorithm.
 *
 * @param <V> the value class
 */
public class RTreeMap<V> extends BTreeMap<SpatialKey, V> {

    /**
     * The spatial key type. 
     */
    final SpatialDataType keyType;

    private boolean quadraticSplit;

    public RTreeMap(String name, int dimensions, StorageDataType valueType, Map<String, Object> config, AOStorage aoStorage) {
        super(name, new SpatialDataType(dimensions), valueType, config, aoStorage);
        this.keyType = (SpatialDataType) getKeyType();
    }

    /**
     * Create a new map with the given dimensions and value type.
     *
     * @param <V> the value type
     * @param dimensions the number of dimensions
     * @param valueType the value type
     * @return the map
     */
    public static <V> RTreeMap<V> create(String name, int dimensions, StorageDataType valueType, Map<String, Object> config,
            AOStorage aoStorage) {
        return new RTreeMap<V>(name, dimensions, valueType, config, aoStorage);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(SpatialKey key) {
        return (V) get(root, key);
    }

    /**
     * Iterate over all keys that have an intersection with the given rectangle.
     *
     * @param x the rectangle
     * @return the iterator
     */
    public RTreeCursor findIntersectingKeys(SpatialKey x) {
        return new RTreeCursor(root, x) {
            @Override
            protected boolean check(boolean leaf, SpatialKey key, SpatialKey test) {
                return keyType.isOverlap(key, test);
            }
        };
    }

    /**
     * Iterate over all keys that are fully contained within the given rectangle.
     *
     * @param x the rectangle
     * @return the iterator
     */
    public RTreeCursor findContainedKeys(SpatialKey x) {
        return new RTreeCursor(root, x) {
            @Override
            protected boolean check(boolean leaf, SpatialKey key, SpatialKey test) {
                if (leaf) {
                    return keyType.isInside(key, test);
                }
                return keyType.isOverlap(key, test);
            }
        };
    }

    private boolean contains(BTreePage p, int index, Object key) {
        return keyType.contains(p.getKey(index), key);
    }

    /**
     * Get the object for the given key. An exact match is required.
     *
     * @param p the page
     * @param key the key
     * @return the value, or null if not found
     */
    protected Object get(BTreePage p, Object key) {
        if (!p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Object o = get(p.getChildPage(i), key);
                    if (o != null) {
                        return o;
                    }
                }
            }
        } else {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    return p.getValue(i);
                }
            }
        }
        return null;
    }

    @Override
    protected synchronized Object remove(BTreePage p, Object key) {
        Object result = null;
        if (p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    result = p.getValue(i);
                    p.remove(i);
                    break;
                }
            }
            return result;
        }
        for (int i = 0; i < p.getKeyCount(); i++) {
            if (contains(p, i, key)) {
                BTreePage cOld = p.getChildPage(i);
                // this will mark the old page as deleted
                // so we need to update the parent in any case
                // (otherwise the old page might be deleted again)
                BTreePage c = cOld.copy();
                long oldSize = c.getTotalCount();
                result = remove(c, key);
                p.setChild(i, c);
                if (oldSize == c.getTotalCount()) {
                    continue;
                }
                if (c.getTotalCount() == 0) {
                    // this child was deleted
                    p.remove(i);
                    if (p.getKeyCount() == 0) {
                        c.removePage();
                    }
                    break;
                }
                Object oldBounds = p.getKey(i);
                if (!keyType.isInside(key, oldBounds)) {
                    p.setKey(i, getBounds(c));
                }
                break;
            }
        }
        return result;
    }

    private Object getBounds(BTreePage x) {
        Object bounds = keyType.createBoundingBox(x.getKey(0));
        for (int i = 1; i < x.getKeyCount(); i++) {
            keyType.increaseBounds(bounds, x.getKey(i));
        }
        return bounds;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V put(SpatialKey key, V value) {
        return (V) putOrAdd(key, value, false);
    }

    /**
     * Add a given key-value pair. The key should not exist (if it exists, the
     * result is undefined).
     *
     * @param key the key
     * @param value the value
     */
    public void add(SpatialKey key, V value) {
        putOrAdd(key, value, true);
    }

    private synchronized Object putOrAdd(SpatialKey key, V value, boolean alwaysAdd) {
        beforeWrite();
        BTreePage p = root.copy();
        Object result;
        if (alwaysAdd || get(key) == null) {
            if (p.getMemory() > storage.getPageSplitSize() && p.getKeyCount() > 3) {
                // only possible if this is the root, else we would have
                // split earlier (this requires pageSplitSize is fixed)
                long totalCount = p.getTotalCount();
                BTreePage split = split(p);
                Object k1 = getBounds(p);
                Object k2 = getBounds(split);
                Object[] keys = { k1, k2 };
                BTreePage.PageReference[] children = { new BTreePage.PageReference(p, p.getPos(), p.getTotalCount()),
                        new BTreePage.PageReference(split, split.getPos(), split.getTotalCount()),
                        new BTreePage.PageReference(null, 0, 0) };
                p = BTreePage.create(this, keys, null, children, totalCount, 0);
                // now p is a node; continues
            }
            add(p, key, value);
            result = null;
        } else {
            result = set(p, key, value);
        }
        newRoot(p);
        return result;
    }

    /**
     * Update the value for the given key. The key must exist.
     *
     * @param p the page
     * @param key the key
     * @param value the new value
     * @return the old value (never null)
     */
    private Object set(BTreePage p, Object key, Object value) {
        if (p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    return p.setValue(i, value);
                }
            }
        } else {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    BTreePage c = p.getChildPage(i);
                    if (get(c, key) != null) {
                        c = c.copy();
                        Object result = set(c, key, value);
                        p.setChild(i, c);
                        return result;
                    }
                }
            }
        }
        throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Not found: {0}", key);
    }

    private void add(BTreePage p, Object key, Object value) {
        if (p.isLeaf()) {
            p.insertLeaf(p.getKeyCount(), key, value);
            return;
        }
        // p is a node
        int index = -1;
        for (int i = 0; i < p.getKeyCount(); i++) {
            if (contains(p, i, key)) {
                index = i;
                break;
            }
        }
        if (index < 0) {
            // a new entry, we don't know where to add yet
            float min = Float.MAX_VALUE;
            for (int i = 0; i < p.getKeyCount(); i++) {
                Object k = p.getKey(i);
                float areaIncrease = keyType.getAreaIncrease(k, key);
                if (areaIncrease < min) {
                    index = i;
                    min = areaIncrease;
                }
            }
        }
        BTreePage c = p.getChildPage(index).copy();
        if (c.getMemory() > storage.getPageSplitSize() && c.getKeyCount() > 4) {
            // split on the way down
            BTreePage split = split(c);
            p.setKey(index, getBounds(c));
            p.setChild(index, c);
            p.insertNode(index, getBounds(split), split);
            // now we are not sure where to add
            add(p, key, value);
            return;
        }
        add(c, key, value);
        Object bounds = p.getKey(index);
        keyType.increaseBounds(bounds, key);
        p.setKey(index, bounds);
        p.setChild(index, c);
    }

    private BTreePage split(BTreePage p) {
        return quadraticSplit ? splitQuadratic(p) : splitLinear(p);
    }

    private BTreePage splitLinear(BTreePage p) {
        ArrayList<Object> keys = New.arrayList();
        for (int i = 0; i < p.getKeyCount(); i++) {
            keys.add(p.getKey(i));
        }
        int[] extremes = keyType.getExtremes(keys);
        if (extremes == null) {
            return splitQuadratic(p);
        }
        BTreePage splitA = newPage(p.isLeaf());
        BTreePage splitB = newPage(p.isLeaf());
        move(p, splitA, extremes[0]);
        if (extremes[1] > extremes[0]) {
            extremes[1]--;
        }
        move(p, splitB, extremes[1]);
        Object boundsA = keyType.createBoundingBox(splitA.getKey(0));
        Object boundsB = keyType.createBoundingBox(splitB.getKey(0));
        while (p.getKeyCount() > 0) {
            Object o = p.getKey(0);
            float a = keyType.getAreaIncrease(boundsA, o);
            float b = keyType.getAreaIncrease(boundsB, o);
            if (a < b) {
                keyType.increaseBounds(boundsA, o);
                move(p, splitA, 0);
            } else {
                keyType.increaseBounds(boundsB, o);
                move(p, splitB, 0);
            }
        }
        while (splitB.getKeyCount() > 0) {
            move(splitB, p, 0);
        }
        return splitA;
    }

    private BTreePage splitQuadratic(BTreePage p) {
        BTreePage splitA = newPage(p.isLeaf());
        BTreePage splitB = newPage(p.isLeaf());
        float largest = Float.MIN_VALUE;
        int ia = 0, ib = 0;
        for (int a = 0; a < p.getKeyCount(); a++) {
            Object objA = p.getKey(a);
            for (int b = 0; b < p.getKeyCount(); b++) {
                if (a == b) {
                    continue;
                }
                Object objB = p.getKey(b);
                float area = keyType.getCombinedArea(objA, objB);
                if (area > largest) {
                    largest = area;
                    ia = a;
                    ib = b;
                }
            }
        }
        move(p, splitA, ia);
        if (ia < ib) {
            ib--;
        }
        move(p, splitB, ib);
        Object boundsA = keyType.createBoundingBox(splitA.getKey(0));
        Object boundsB = keyType.createBoundingBox(splitB.getKey(0));
        while (p.getKeyCount() > 0) {
            float diff = 0, bestA = 0, bestB = 0;
            int best = 0;
            for (int i = 0; i < p.getKeyCount(); i++) {
                Object o = p.getKey(i);
                float incA = keyType.getAreaIncrease(boundsA, o);
                float incB = keyType.getAreaIncrease(boundsB, o);
                float d = Math.abs(incA - incB);
                if (d > diff) {
                    diff = d;
                    bestA = incA;
                    bestB = incB;
                    best = i;
                }
            }
            if (bestA < bestB) {
                keyType.increaseBounds(boundsA, p.getKey(best));
                move(p, splitA, best);
            } else {
                keyType.increaseBounds(boundsB, p.getKey(best));
                move(p, splitB, best);
            }
        }
        while (splitB.getKeyCount() > 0) {
            move(splitB, p, 0);
        }
        return splitA;
    }

    private BTreePage newPage(boolean leaf) {
        Object[] values;
        BTreePage.PageReference[] refs;
        if (leaf) {
            values = BTreePage.EMPTY_OBJECT_ARRAY;
            refs = null;
        } else {
            values = null;
            refs = new BTreePage.PageReference[] { new BTreePage.PageReference(null, 0, 0) };
        }
        return BTreePage.create(this, BTreePage.EMPTY_OBJECT_ARRAY, values, refs, 0, 0);
    }

    private static void move(BTreePage source, BTreePage target, int sourceIndex) {
        Object k = source.getKey(sourceIndex);
        if (source.isLeaf()) {
            Object v = source.getValue(sourceIndex);
            target.insertLeaf(0, k, v);
        } else {
            BTreePage c = source.getChildPage(sourceIndex);
            target.insertNode(0, k, c);
        }
        source.remove(sourceIndex);
    }

    /**
     * Add all node keys (including internal bounds) to the given list.
     * This is mainly used to visualize the internal splits.
     *
     * @param list the list
     * @param p the root page
     */
    public void addNodeKeys(ArrayList<SpatialKey> list, BTreePage p) {
        if (p != null && !p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                list.add((SpatialKey) p.getKey(i));
                addNodeKeys(list, p.getChildPage(i));
            }
        }
    }

    public boolean isQuadraticSplit() {
        return quadraticSplit;
    }

    public void setQuadraticSplit(boolean quadraticSplit) {
        this.quadraticSplit = quadraticSplit;
    }

    @Override
    protected int getChildPageCount(BTreePage p) {
        return p.getRawChildPageCount() - 1;
    }

    /**
     * A cursor to iterate over a subset of the keys.
     */
    public static class RTreeCursor implements Iterator<SpatialKey> {

        private final SpatialKey filter;
        private CursorPos pos;
        private SpatialKey current;
        private final BTreePage root;
        private boolean initialized;

        protected RTreeCursor(BTreePage root, SpatialKey filter) {
            this.root = root;
            this.filter = filter;
        }

        @Override
        public boolean hasNext() {
            if (!initialized) {
                // init
                pos = new CursorPos(root, 0, null);
                fetchNext();
                initialized = true;
            }
            return current != null;
        }

        /**
         * Skip over that many entries. This method is relatively fast (for this
         * map implementation) even if many entries need to be skipped.
         *
         * @param n the number of entries to skip
         */
        public void skip(long n) {
            while (hasNext() && n-- > 0) {
                fetchNext();
            }
        }

        @Override
        public SpatialKey next() {
            if (!hasNext()) {
                return null;
            }
            SpatialKey c = current;
            fetchNext();
            return c;
        }

        @Override
        public void remove() {
            throw DataUtils.newUnsupportedOperationException("Removing is not supported");
        }

        /**
         * Fetch the next entry if there is one.
         */
        protected void fetchNext() {
            while (pos != null) {
                BTreePage p = pos.page;
                if (p.isLeaf()) {
                    while (pos.index < p.getKeyCount()) {
                        SpatialKey c = (SpatialKey) p.getKey(pos.index++);
                        if (filter == null || check(true, c, filter)) {
                            current = c;
                            return;
                        }
                    }
                } else {
                    boolean found = false;
                    while (pos.index < p.getKeyCount()) {
                        int index = pos.index++;
                        SpatialKey c = (SpatialKey) p.getKey(index);
                        if (filter == null || check(false, c, filter)) {
                            BTreePage child = pos.page.getChildPage(index);
                            pos = new CursorPos(child, 0, pos);
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        continue;
                    }
                }
                // parent
                pos = pos.parent;
            }
            current = null;
        }

        /**
         * Check a given key.
         *
         * @param leaf if the key is from a leaf page
         * @param key the stored key
         * @param test the user-supplied test key
         * @return true if there is a match
         */
        protected boolean check(boolean leaf, SpatialKey key, SpatialKey test) {
            return true;
        }

    }

    @Override
    public String getType() {
        return "RTree";
    }

    /**
     * A builder for this class.
     */
    public static class Builder<V> extends StorageMapBuilder<RTreeMap<V>, SpatialKey, V> {
        private int dimensions = 2;

        /**
         * Create a new builder for maps with 2 dimensions.
         */
        public Builder() {
            // default
        }

        /**
         * Set the dimensions.
         *
         * @param dimensions the dimensions to use
         * @return this
         */
        public Builder<V> dimensions(int dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        @Override
        public RTreeMap<V> openMap() {
            return new RTreeMap<V>(name, dimensions, valueType, config, aoStorage);
        }
    }
}

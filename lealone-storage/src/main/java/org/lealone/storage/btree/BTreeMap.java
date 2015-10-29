/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.storage.btree;

import java.util.Map;

import org.lealone.common.util.DataUtils;
import org.lealone.storage.AOStorage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapBuilder;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.ObjectDataType;

/**
 * A read optimization BTree stored map.
 * <p>
 * Read operations can happen concurrently with all other operations, without
 * risk of corruption.
 * <p>
 * Write operations first read the relevant area from disk to memory
 * concurrently, and only then modify the data. The in-memory part of write
 * operations is synchronized. For scalable concurrent in-memory write
 * operations, the map should be split into multiple smaller sub-maps that are
 * then synchronized independently.
 * 
 * @param <K> the key class
 * @param <V> the value class
 * 
 * @author H2 Group
 * @author zhh
 */
public class BTreeMap<K, V> implements StorageMap<K, V> {

    /**
     * A builder for this class.
     */
    public static class Builder<K, V> extends StorageMapBuilder<BTreeMap<K, V>, K, V> {
        @Override
        public BTreeMap<K, V> openMap() {
            return new BTreeMap<>(name, keyType, valueType, config, aoStorage);
        }
    }

    protected final String name;
    protected final DataType keyType;
    protected final DataType valueType;
    protected final boolean readOnly;

    protected final Map<String, Object> config;
    protected final BTreeStorage storage;
    protected final AOStorage aoStorage;

    /**
     * The current root page (may not be null).
     */
    protected volatile BTreePage root;

    @SuppressWarnings("unchecked")
    protected BTreeMap(String name, DataType keyType, DataType valueType, Map<String, Object> config,
            AOStorage aoStorage) {
        DataUtils.checkArgument(name != null, "The name may not be null");
        DataUtils.checkArgument(config != null, "The config may not be null");

        if (keyType == null) {
            keyType = new ObjectDataType();
        }
        if (valueType == null) {
            valueType = new ObjectDataType();
        }

        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
        this.readOnly = config.containsKey("readOnly");
        this.config = config;
        this.aoStorage = aoStorage;

        storage = new BTreeStorage((BTreeMap<Object, Object>) this);

        if (storage.lastChunk != null)
            root = storage.readPage(storage.lastChunk.rootPagePos);
        else
            root = BTreePage.createEmpty(this);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public DataType getKeyType() {
        return keyType;
    }

    @Override
    public DataType getValueType() {
        return valueType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        return (V) binarySearch(root, key);
    }

    /**
     * Get the value for the given key, or null if not found.
     * 
     * @param p the page
     * @param key the key
     * @return the value or null
     */
    protected Object binarySearch(BTreePage p, Object key) {
        int index = p.binarySearch(key);
        if (!p.isLeaf()) {
            if (index < 0) {
                index = -index - 1;
            } else {
                index++;
            }
            p = p.getChildPage(index);
            return binarySearch(p, key);
        }
        if (index >= 0) {
            return p.getValue(index);
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized V put(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        beforeWrite();
        BTreePage p = root.copy();

        if (p.needSplit())
            p = splitRoot(p);

        Object result = put(p, key, value);
        newRoot(p);
        return (V) result;
    }

    /**
     * This method is called before writing to the map. 
     * The default implementation checks whether writing is allowed.
     * 
     * @throws UnsupportedOperationException if the map is read-only.
     */
    protected void beforeWrite() {
        if (storage.isClosed()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "This map is closed");
        }
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException("This map is read-only");
        }
    }

    /**
     * Split the root page.
     * 
     * @param p the page
     * @return the new sibling
     */
    protected BTreePage splitRoot(BTreePage p) {
        long totalCount = p.getTotalCount();
        int at = p.getKeyCount() / 2;
        Object k = p.getKey(at);
        BTreePage split = p.split(at);
        Object[] keys = { k };
        BTreePage.PageReference[] children = { new BTreePage.PageReference(p, p.getPos(), p.getTotalCount()),
                new BTreePage.PageReference(split, split.getPos(), split.getTotalCount()), };
        p = BTreePage.create(this, keys, null, children, totalCount, 0);
        return p;
    }

    /**
     * Add or update a key-value pair.
     * 
     * @param p the page
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value, or null
     */
    protected Object put(BTreePage p, Object key, Object value) {
        int index = p.binarySearch(key);
        if (p.isLeaf()) {
            if (index < 0) {
                index = -index - 1;
                p.insertLeaf(index, key, value);
                return null;
            }
            return p.setValue(index, value);
        }
        // p is a node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        BTreePage c = p.getChildPage(index).copy();
        if (c.needSplit()) {
            // split on the way down
            int at = c.getKeyCount() / 2;
            Object k = c.getKey(at);
            BTreePage split = c.split(at);
            p.setChild(index, split);
            p.insertNode(index, k, c);
            // now we are not sure where to add
            return put(p, key, value);
        }
        Object result = put(c, key, value);
        p.setChild(index, c);
        return result;
    }

    /**
     * Use the new root page from now on.
     * 
     * @param newRoot the new root page
     */
    protected void newRoot(BTreePage newRoot) {
        if (root != newRoot) {
            root = newRoot;
        }
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        V old = get(key);
        if (old == null) {
            put(key, value);
        }
        return old;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        beforeWrite();
        V result = get(key);
        if (result == null) {
            return null;
        }
        synchronized (this) {
            BTreePage p = root.copy();
            result = (V) remove(p, key);
            if (!p.isLeaf() && p.getTotalCount() == 0) {
                p.removePage();
                p = BTreePage.createEmpty(this);
            }
            newRoot(p);
        }
        return result;
    }

    /**
     * Remove a key-value pair.
     * 
     * @param p the page (may not be null)
     * @param key the key
     * @return the old value, or null if the key did not exist
     */
    protected Object remove(BTreePage p, Object key) {
        int index = p.binarySearch(key);
        Object result = null;
        if (p.isLeaf()) {
            if (index >= 0) {
                result = p.getValue(index);
                p.remove(index);
            }
            return result;
        }
        // node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        BTreePage cOld = p.getChildPage(index);
        BTreePage c = cOld.copy();
        result = remove(c, key);
        if (result == null || c.getTotalCount() != 0) {
            // no change, or
            // there are more nodes
            p.setChild(index, c);
        } else {
            // this child was deleted
            if (p.getKeyCount() == 0) {
                p.setChild(index, c);
                c.removePage();
            } else {
                p.remove(index);
            }
        }
        return result;
    }

    @Override
    public synchronized boolean replace(K key, V oldValue, V newValue) {
        V old = get(key);
        if (areValuesEqual(old, oldValue)) {
            put(key, newValue);
            return true;
        }
        return false;
    }

    @Override
    public K firstKey() {
        return getFirstLast(true);
    }

    @Override
    public K lastKey() {
        return getFirstLast(false);
    }

    /**
     * Get the first (lowest) or last (largest) key.
     * 
     * @param first whether to retrieve the first key
     * @return the key, or null if the map is empty
     */
    @SuppressWarnings("unchecked")
    protected K getFirstLast(boolean first) {
        if (sizeAsLong() == 0) {
            return null;
        }
        BTreePage p = root;
        while (true) {
            if (p.isLeaf()) {
                return (K) p.getKey(first ? 0 : p.getKeyCount() - 1);
            }
            p = p.getChildPage(first ? 0 : getChildPageCount(p) - 1);
        }
    }

    @Override
    public K lowerKey(K key) {
        return getMinMax(key, true, true);
    }

    @Override
    public K floorKey(K key) {
        return getMinMax(key, true, false);
    }

    @Override
    public K higherKey(K key) {
        return getMinMax(key, false, true);
    }

    @Override
    public K ceilingKey(K key) {
        return getMinMax(key, false, false);
    }

    /**
     * Get the smallest or largest key using the given bounds.
     * 
     * @param key the key
     * @param min whether to retrieve the smallest key
     * @param excluding if the given upper/lower bound is exclusive
     * @return the key, or null if no such key exists
     */
    protected K getMinMax(K key, boolean min, boolean excluding) {
        return getMinMax(root, key, min, excluding);
    }

    @SuppressWarnings("unchecked")
    private K getMinMax(BTreePage p, K key, boolean min, boolean excluding) {
        if (p.isLeaf()) {
            int x = p.binarySearch(key);
            if (x < 0) {
                x = -x - (min ? 2 : 1);
            } else if (excluding) {
                x += min ? -1 : 1;
            }
            if (x < 0 || x >= p.getKeyCount()) {
                return null;
            }
            return (K) p.getKey(x);
        }
        int x = p.binarySearch(key);
        if (x < 0) {
            x = -x - 1;
        } else {
            x++;
        }
        while (true) {
            if (x < 0 || x >= getChildPageCount(p)) {
                return null;
            }
            K k = getMinMax(p.getChildPage(x), key, min, excluding);
            if (k != null) {
                return k;
            }
            x += min ? -1 : 1;
        }
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return false;
        }
        return valueType.compare(a, b) == 0;
    }

    @Override
    public int size() {
        long size = sizeAsLong();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    @Override
    public long sizeAsLong() {
        return root.getTotalCount();
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean isEmpty() {
        return sizeAsLong() == 0;
    }

    @Override
    public boolean isInMemory() {
        return false;
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return new BTreeCursor<>(this, root, from);
    }

    @Override
    public synchronized void clear() {
        beforeWrite();
        root.removeAllRecursive();
        newRoot(BTreePage.createEmpty(this));
    }

    @Override
    public void remove() {
        storage.remove();
        aoStorage.closeMap(name);
    }

    @Override
    public boolean isClosed() {
        return storage.isClosed();
    }

    @Override
    public void close() {
        storage.close();
        aoStorage.closeMap(name);
    }

    @Override
    public void save() {
        storage.save();
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public BTreeStorage getStorage() {
        return storage;
    }

    /**
     * Get the child page count for this page. This is to allow another map
     * implementation to override the default, in case the last child is not to
     * be used.
     * 
     * @param p the page
     * @return the number of direct children
     */
    protected int getChildPageCount(BTreePage p) {
        return p.getRawChildPageCount();
    }

    protected String getType() {
        return "BTree";
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        DataUtils.appendMap(buff, "name", name);
        String type = getType();
        if (type != null) {
            DataUtils.appendMap(buff, "type", type);
        }
        return buff.toString();
    }

    public void printPage() {
        printPage(true);
    }

    public void printPage(boolean readOffLinePage) {
        System.out.println(root.getPrettyPageInfo(readOffLinePage));
    }

}
